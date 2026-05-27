# Para servidor local sin limites VM horario

from flask import Flask, request, jsonify, render_template_string, send_file, Response
import threading
import requests
import pandas as pd
import datetime
import numpy as np
import time
import io
import zipfile
import json
import logging
import urllib.parse

selected_cols = [
    "PM25_1h", "PM25raw_1h", "PM1_1h", "Humidity_1h", "Temperature_1h",
]

# Initialize Flask app and set logging level
app = Flask(__name__)
app.logger.setLevel(logging.DEBUG)

# Global lock for one-at-a-time processing
processing_lock = threading.Lock()

# Generador Mágico: Descarga por bloques de 90 días y suelta la memoria
def fetch_vm_data_generator(base_url, query, selected_cols, start_datetime, end_datetime):
    chunk_size = datetime.timedelta(days=90)
    current_start = start_datetime

    while current_start < end_datetime:
        current_end = min(current_start + chunk_size, end_datetime)
        
        start_str = current_start.isoformat() + "Z"
        end_str = current_end.isoformat() + "Z"
        
        query_url = f"{base_url}/query_range?query={urllib.parse.quote(query)}&start={start_str}&end={end_str}&step=1h"
        
        app.logger.debug(f"Querying VictoriaMetrics chunk from {start_str} to {end_str}")
        
        chunk_data = []
        try:
            response = requests.get(query_url)
            if response.status_code == 422:
                app.logger.error(f"VM Limit Error Detail: {response.text}")
                
            response.raise_for_status()
            data = response.json().get('data', {}).get('result', [])

            if data:
                df = pd.json_normalize(data)
                
                if 'metric.job' in df.columns and 'values' in df.columns:
                    df = df.explode('values')
                    # Mantenemos la fecha como String directamente. Ahorra 90% de CPU
                    df['date'] = df['values'].apply(lambda x: datetime.datetime.fromtimestamp(x[0], tz=datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'))
                    df['value'] = df['values'].apply(lambda x: float(x[1]))
                    df = df.drop(columns="values")

                    df = df.rename(columns={"metric.__name__": "metric_name", "metric.job": "station"})
                    df = df[df['station'].notnull()]

                    if not df.empty:
                        # pivot_table previene errores si un sensor mandó datos dobles
                        df_result = pd.pivot_table(df, index=['station', 'date'], columns='metric_name', values='value', aggfunc='mean').reset_index()
                        chunk_data.append(df_result)
                        
        except Exception as e:
            app.logger.error(f'Error processing chunk: {str(e)}')
            pass 

        # Si hubo datos en este bloque, los "rendimos" (yield) a Flask
        if chunk_data:
            final_chunk_df = pd.concat(chunk_data, ignore_index=True)
            chunk_data.clear() # Limpia la RAM inmediatamente
            
            for col in selected_cols:
                if col not in final_chunk_df.columns:
                    final_chunk_df[col] = np.nan
            yield current_start, final_chunk_df
        else:
            yield current_start, pd.DataFrame(columns=['station', 'date'] + selected_cols)

        current_start = current_end

@app.route('/getdata')
def index():
    variables = request.args.getlist('variables') or selected_cols
    
    now = datetime.datetime.now()
    one_week_ago = now - datetime.timedelta(days=7)
    
    start_date = request.args.get('start_date', one_week_ago.strftime('%Y-%m-%d'))
    start_time = request.args.get('start_time', '00:00')
    end_date = request.args.get('end_date', now.strftime('%Y-%m-%d'))
    end_time = request.args.get('end_time', '23:00')
    
    station_filter = request.args.get('station_filter', '')

    return render_template_string('''
        <!DOCTYPE html>
        <html>
        <head>
            <title>API AireCiudadano Horario 1h</title>
            <style>
                body { font-family: Arial, sans-serif; max-width: 600px; margin: 20px auto; }
                .alert { padding: 15px; margin-bottom: 20px; border: 1px solid transparent; border-radius: 4px; display: none; }
                .alert-info { color: #31708f; background-color: #d9edf7; border-color: #bce8f1; display: block; }
                #status_message { margin-top: 15px; font-size: 16px; }
            </style>
        </head>
        <body>
            <form id="dataForm" action="/dataresult" method="post">
                <h2>API AIRECIUDADANO v2.0 (VM - Hourly Averages)</h2>
                <div class="alert alert-info">
                    <strong>New!</strong> Download up to 1 year of hourly data instantly using the CSV format. Large JSON downloads will be bundled in 90-day chunks to preserve memory
                </div>
                
                <label><b>Select variables:</b></label><br><br>
                {% for col in selected_cols %}
                    <input type="checkbox" id="{{ col }}" name="variables" value="{{ col }}" {% if col in variables %}checked{% endif %}>
                    <label for="{{ col }}">{{ col }}</label><br>
                {% endfor %}
                <br>
                
                <label>Start date/time (UTC+0/GMT0):</label>
                <input type="date" name="start_date" value="{{ start_date }}" required>
                <input type="time" name="start_time" value="{{ start_time }}" step="3600" required><br><br>
                
                <label>End date/time (UTC+0/GMT0):</label>
                <input type="date" name="end_date" value="{{ end_date }}" required>
                <input type="time" name="end_time" value="{{ end_time }}" step="3600" required><br><br>
                
                <label>Station Filter (Comma separated):</label>
                <input type="text" name="station_filter" value="{{ station_filter }}" style="width: 100%;"><br><br>

                <label>Result format:</label>
                <select id="result_format" name="result_format">
                    <option value="screen">Result in screen (Max 7 days)</option>
                    <option value="filejson">Result in ZIP JSON (Max 6 months)</option>
                    <option value="filecsv">Result in ZIP CSV (Max 1 year)</option>
                </select><br><br>

                <input type="submit" id="submitBtn" value="Download Data" style="padding: 10px 20px; background: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer;">
                
                <div id="status_message"></div>
            </form>

            <script>
                document.getElementById('dataForm').addEventListener('submit', function(event) {
                    const format = document.getElementById('result_format').value;
                    const statusDiv = document.getElementById('status_message');
                    const btn = document.getElementById('submitBtn');
                    
                    if (format === 'screen') {
                        statusDiv.innerHTML = '<span style="color: blue;"><b>Processing your request...</b></span>';
                        return true; 
                    }

                    event.preventDefault();
                    statusDiv.innerHTML = '<span style="color: blue;"><b>⏳ Processing your request... Extracting and compressing data chunk by chunk, this may take a moment.</b></span>';
                    btn.disabled = true;
                    btn.style.background = '#ccc';

                    fetch('/dataresult', {
                        method: 'POST',
                        body: new FormData(event.target)
                    }).then(async response => {
                        btn.disabled = false;
                        btn.style.background = '#007bff';
                        
                        const contentType = response.headers.get('content-type');
                        
                        if (contentType && contentType.includes('application/json')) {
                            const data = await response.json();
                            if (data.error) {
                                statusDiv.innerHTML = '<span style="color: red;"><b>❌ Error:</b> ' + data.error + '</span>';
                            } else if (data.message) {
                                statusDiv.innerHTML = '<span style="color: orange;"><b>⚠️ Notice:</b> ' + data.message + '</span>';
                            }
                        } else {
                            const blob = await response.blob();
                            const url = window.URL.createObjectURL(blob);
                            const a = document.createElement('a');
                            a.href = url;
                            
                            let filename = 'data_export_hourly.zip';
                            const disp = response.headers.get('Content-Disposition');
                            if (disp && disp.includes('filename=')) {
                                filename = disp.split('filename=')[1].replace(/"/g, '');
                            }
                            
                            a.download = filename;
                            document.body.appendChild(a);
                            a.click();
                            a.remove();
                            window.URL.revokeObjectURL(url);
                            
                            statusDiv.innerHTML = '<span style="color: green;"><b>✅ Success! Your data has been downloaded automatically.</b></span>';
                        }
                    }).catch(err => {
                        btn.disabled = false;
                        btn.style.background = '#007bff';
                        statusDiv.innerHTML = '<span style="color: red;"><b>❌ Network error:</b> ' + err.message + '</span>';
                    });
                });
            </script>
        </body>
        </html>
    ''', selected_cols=selected_cols, variables=variables, start_date=start_date,
       start_time=start_time, end_date=end_date, end_time=end_time, station_filter=station_filter)

@app.route('/dataresult', methods=['POST'])
def data():
    if not processing_lock.acquire(blocking=False):
        return jsonify({
            'error': 'The API is currently processing another request. Please wait and try again shortly.'
        })

    start_time_proc = time.time()
    try:
        variables = request.form.getlist('variables')
        base_url = "http://sensor.aireciudadano.com:30001/api/v1"

        start_date = request.form['start_date']
        start_time_str = request.form['start_time']
        end_date = request.form['end_date']
        end_time = request.form['end_time']
        station_filter = request.form.get('station_filter', '')
        result_format = request.form.get('result_format', 'screen')

        start_datetime = datetime.datetime.fromisoformat(f"{start_date}T{start_time_str}")
        end_datetime = datetime.datetime.fromisoformat(f"{end_date}T{end_time}")
        
        # Validation Limits
        date_diff = end_datetime - start_datetime
        
        if result_format == 'screen':
            if date_diff.days > 7:
                processing_lock.release()
                return jsonify({
                    'error': 'For screen visualization, the maximum limit is 7 days to prevent browser freezing. Please reduce the date range or select JSON/CSV file format.'
                })
        elif result_format == 'filejson':
            if date_diff.days > 183:
                processing_lock.release()
                return jsonify({
                    'error': 'For JSON file downloads, the maximum limit is 6 months (183 days) due to data size constraints. Please reduce the date range or select CSV format.'
                })
        elif result_format == 'filecsv':
            if date_diff.days > 366:
                processing_lock.release()
                return jsonify({
                    'error': 'The maximum limit for CSV file downloads is 1 full year (365 days). Please reduce the date range.'
                })

        metrics_regex = "|".join(variables)
        if station_filter:
            station_regex = station_filter.replace(',', '|')
            query = f'{{__name__=~"{metrics_regex}", job=~".*({station_regex}).*"}}'
        else:
            query = f'{{__name__=~"{metrics_regex}"}}'

        # ==========================================
        # RUTA PANTALLA: Carga completa en RAM
        # ==========================================
        if result_format == 'screen':
            # Solo permite 7 días, así que podemos cargarlo todo a la vez
            obs = fetch_vm_data_generator(base_url, query, variables, start_datetime, end_datetime)
            # Extrayendo el único chunk esperado
            for chunk_start, chunk_df in obs:
                if chunk_df.empty:
                    processing_lock.release()
                    return jsonify({'message': 'No data found for the selected period and stations.'})
                
                chunk_df = chunk_df.sort_values(by=['station', 'date'])
                
                for col in variables:
                    if col in chunk_df.columns:
                        chunk_df[col] = pd.to_numeric(chunk_df[col], errors='coerce').round(3)
                        
                chunk_df = chunk_df.replace({np.nan: None})
                
                grouped_data = {
                    station: group.drop(columns=['station']).to_dict(orient='records') 
                    for station, group in chunk_df.groupby('station')
                }
                
                process_duration = time.time() - start_time_proc
                hours, remainder = divmod(int(process_duration), 3600)
                minutes, seconds = divmod(remainder, 60)
                formatted_duration = f"{hours}:{minutes:02}:{seconds:02}"
                
                result_data = {
                    'total_records': chunk_df.shape[0],
                    'data': grouped_data,
                    'process_duration': formatted_duration
                }
                
                processing_lock.release()
                return jsonify(result_data)

        # ==========================================
        # RUTAS ARCHIVO: Streaming a ZIP por Chunks
        # ==========================================
        memory_file = io.BytesIO()
        data_found_in_any_chunk = False
        
        with zipfile.ZipFile(memory_file, 'w', zipfile.ZIP_DEFLATED) as zf:
            
            for chunk_start, chunk_df in fetch_vm_data_generator(base_url, query, variables, start_datetime, end_datetime):
                
                if chunk_df.empty:
                    continue
                    
                data_found_in_any_chunk = True
                
                # Nombre del archivo basado en el inicio del chunk de 90 días
                chunk_str = chunk_start.strftime('%Y-%m-%d')
                
                chunk_df = chunk_df.sort_values(by=['station', 'date'])

                for col in variables:
                    if col in chunk_df.columns:
                        chunk_df[col] = pd.to_numeric(chunk_df[col], errors='coerce').round(3)

                total_records = chunk_df.shape[0]

                if result_format == 'filecsv':
                    csv_buffer = io.StringIO()
                    chunk_df.to_csv(csv_buffer, index=False)
                    zf.writestr(f'data_hourly_{chunk_str}.csv', csv_buffer.getvalue())
                    csv_buffer.close()
                
                elif result_format == 'filejson':
                    with zf.open(f'data_hourly_{chunk_str}.json', 'w') as json_file:
                        json_file.write(f'{{\n  "total_records": {total_records},\n'.encode('utf-8'))
                        json_file.write(f'  "chunk_start": "{chunk_start.isoformat()}Z",\n'.encode('utf-8'))
                        json_file.write(b'  "data": {\n')
                        
                        first_station = True
                        for station, group in chunk_df.groupby('station'):
                            if not first_station:
                                json_file.write(b',\n')
                            first_station = False
                            
                            json_file.write(f'    "{station}": '.encode('utf-8'))
                            json_str = group.drop(columns=['station']).to_json(orient='records')
                            json_file.write(json_str.encode('utf-8'))
                        
                        json_file.write(b'\n  }\n}')
                
                # Forzamos a Python a destruir este bloque de la memoria RAM de inmediato
                del chunk_df

        if not data_found_in_any_chunk:
            processing_lock.release()
            return jsonify({'message': 'No data found for the selected period and stations.'})

        memory_file.seek(0)
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'data_hourly_{start_date}_to_{end_date}_{timestamp}.zip'

        processing_lock.release()
        
        return Response(
            memory_file.getvalue(),
            mimetype='application/zip',
            headers={
                'Content-Disposition': f'attachment; filename="{filename}"',
                'Access-Control-Expose-Headers': 'Content-Disposition'
            }
        )

    except Exception as e:
        if processing_lock.locked():
            processing_lock.release()
        app.logger.error(f'Error en endpoint de datos: {str(e)}')
        return jsonify({'error': f'Internal Server Error: {str(e)}'})

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8084)