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

# Variables min
selected_cols = [
    "PM25", "PM25raw", "PM1", "Humidity", "Temperature",
]

# Initialize Flask app and set logging level
app = Flask(__name__)
app.logger.setLevel(logging.DEBUG)

# Global lock for one-at-a-time processing
processing_lock = threading.Lock()

# Generador Mágico: Descarga por horas, agrupa por días y "suelta" la memoria
def fetch_vm_data_daily_generator(base_url, query, selected_cols, start_datetime, end_datetime):
    daily_chunk = datetime.timedelta(days=1)
    current_day = start_datetime

    while current_day < end_datetime:
        current_day_end = min(current_day + daily_chunk, end_datetime)
        
        # Bloques de 1 hora para proteger VictoriaMetrics
        micro_chunk = datetime.timedelta(hours=1)
        current_micro = current_day
        daily_data = []

        while current_micro < current_day_end:
            micro_end = min(current_micro + micro_chunk, current_day_end)
            
            start_str = current_micro.isoformat() + "Z"
            end_str = micro_end.isoformat() + "Z"
            
            query_url = f"{base_url}/query_range?query={urllib.parse.quote(query)}&start={start_str}&end={end_str}&step=1m"
            app.logger.debug(f"Querying VM chunk: {start_str} to {end_str}")
            
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
                        df['date'] = df['values'].apply(lambda x: datetime.datetime.fromtimestamp(x[0], tz=datetime.timezone.utc).isoformat())
                        df['value'] = df['values'].apply(lambda x: float(x[1]))
                        df = df.drop(columns="values")

                        df = df.rename(columns={"metric.__name__": "metric_name", "metric.job": "station"})
                        df = df[df['station'].notnull()]

                        if not df.empty:
                            df_result = pd.pivot_table(df, index=['station', 'date'], columns='metric_name', values='value', aggfunc='mean').reset_index()
                            daily_data.append(df_result)
                            
            except Exception as e:
                app.logger.error(f'Error processing micro-chunk: {str(e)}')
                pass 

            current_micro = micro_end

        # Si hubo datos en este día, los empacamos y "rendimos" (yield) a Flask
        if daily_data:
            final_daily_df = pd.concat(daily_data, ignore_index=True)
            for col in selected_cols:
                if col not in final_daily_df.columns:
                    final_daily_df[col] = np.nan
            yield current_day, final_daily_df
        else:
            yield current_day, pd.DataFrame(columns=['station', 'date'] + selected_cols)
            
        # Al pasar al siguiente ciclo, la RAM del día anterior se limpia sola
        current_day = current_day_end


@app.route('/getdata')
def index():
    variables = request.args.getlist('variables') or selected_cols
    
    now = datetime.datetime.now()
    one_day_ago = now - datetime.timedelta(days=1)
    
    start_date = request.args.get('start_date', one_day_ago.strftime('%Y-%m-%d'))
    start_time = request.args.get('start_time', now.strftime('%H:00'))
    end_date = request.args.get('end_date', now.strftime('%Y-%m-%d'))
    end_time = request.args.get('end_time', now.strftime('%H:00'))
    
    station_filter = request.args.get('station_filter', '')

    return render_template_string('''
        <!DOCTYPE html>
        <html>
        <head>
            <title>API AireCiudadano Minutal 1m</title>
            <style>
                body { font-family: Arial, sans-serif; max-width: 600px; margin: 20px auto; }
                .alert { padding: 15px; margin-bottom: 20px; border: 1px solid transparent; border-radius: 4px; display: none; }
                .alert-warning { color: #8a6d3b; background-color: #fcf8e3; border-color: #faebcc; display: block; }
                #status_message { margin-top: 15px; font-size: 16px; }
            </style>
        </head>
        <body>
            <form id="dataForm" action="/dataresult" method="post">
                <h2>API AIRECIUDADANO v2.0 (VM - RAW 1m)</h2>
                <div class="alert alert-warning">
                    <strong>Notice:</strong> This endpoint retrieves raw, minute-by-minute data. Due to the massive data volume, <b>the maximum download limit is 7 days</b> for all formats.
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
                    <option value="filejson">Result in ZIP JSON (Max 7 days)</option>
                    <option value="filecsv">Result in ZIP CSV (Max 7 days)</option>
                </select><br><br>

                <input type="submit" id="submitBtn" value="Download Data" style="padding: 10px 20px; background: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer;">
                
                <div id="status_message"></div>
            </form>

            <script>
                document.getElementById('dataForm').addEventListener('submit', function(event) {
                    const statusDiv = document.getElementById('status_message');
                    const btn = document.getElementById('submitBtn');

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
                            
                            let filename = 'data_export_raw.zip';
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
        result_format = request.form.get('result_format', 'filejson')

        start_datetime = datetime.datetime.fromisoformat(f"{start_date}T{start_time_str}")
        end_datetime = datetime.datetime.fromisoformat(f"{end_date}T{end_time}")
        
        # Validación de Límites a 7 días unificado
        date_diff = end_datetime - start_datetime
        
        if date_diff.days > 7:
            processing_lock.release()
            return jsonify({
                'error': 'The maximum limit for raw minute data downloads is 7 days due to server constraints. Please reduce the date range.'
            })

        metrics_regex = "|".join(variables)
        if station_filter:
            station_regex = station_filter.replace(',', '|')
            query = f'{{__name__=~"{metrics_regex}", job=~".*({station_regex}).*"}}'
        else:
            query = f'{{__name__=~"{metrics_regex}"}}'

        memory_file = io.BytesIO()
        data_found_in_any_day = False
        
        # Abrimos el ZIP una sola vez y vamos metiendo archivos día a día
        with zipfile.ZipFile(memory_file, 'w', zipfile.ZIP_DEFLATED) as zf:
            
            # EL BUCLE MAGICO: Genera 1 bloque -> Escribe al Zip -> Libera RAM -> Repite
            for day_start, daily_df in fetch_vm_data_daily_generator(base_url, query, variables, start_datetime, end_datetime):
                
                if daily_df.empty:
                    continue
                    
                data_found_in_any_day = True
                
                # Nombre del archivo incluye la fecha y hora de inicio del bloque
                # Ejemplo: 2026-05-15_1000
                day_str = day_start.strftime('%Y-%m-%d_%H%M')
                
                # OPTIMIZACIÓN EXTREMA: Ordenamiento alfabético directo de ISO strings
                daily_df = daily_df.sort_values(by=['station', 'date'])

                for col in variables:
                    if col in daily_df.columns:
                        daily_df[col] = pd.to_numeric(daily_df[col], errors='coerce').round(3)

                total_records = daily_df.shape[0]

                if result_format == 'filecsv':
                    csv_buffer = io.StringIO()
                    daily_df.to_csv(csv_buffer, index=False)
                    # Guarda el archivo en el zip con el nombre de su fecha y hora
                    zf.writestr(f'data_{day_str}.csv', csv_buffer.getvalue())
                    csv_buffer.close()
                
                elif result_format == 'filejson':
                    with zf.open(f'data_{day_str}.json', 'w') as json_file:
                        json_file.write(f'{{\n  "total_records": {total_records},\n'.encode('utf-8'))
                        json_file.write(f'  "date_start": "{day_start.isoformat()}Z",\n'.encode('utf-8'))
                        json_file.write(b'  "data": {\n')
                        
                        first_station = True
                        for station, group in daily_df.groupby('station'):
                            if not first_station:
                                json_file.write(b',\n')
                            first_station = False
                            
                            json_file.write(f'    "{station}": '.encode('utf-8'))
                            # to_json elimina NaNs nativamente, sin bloqueos de RAM
                            json_str = group.drop(columns=['station']).to_json(orient='records')
                            json_file.write(json_str.encode('utf-8'))
                        
                        json_file.write(b'\n  }\n}')
                
                # Forzamos a Python a destruir este día de la memoria RAM de inmediato
                del daily_df

        if not data_found_in_any_day:
            processing_lock.release()
            return jsonify({'message': 'No data found for the selected period and stations.'})

        memory_file.seek(0)
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'data_raw_{start_date}_to_{end_date}_{timestamp}.zip'

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