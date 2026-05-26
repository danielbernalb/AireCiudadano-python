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

# Fast-track VM fetch with chunking to avoid 422 limit and pivot_table to avoid duplicates
def fetch_vm_data(base_url, query, selected_cols, start_datetime, end_datetime):
    chunk_size = datetime.timedelta(days=90)
    current_start = start_datetime
    all_data = []

    while current_start < end_datetime:
        current_end = min(current_start + chunk_size, end_datetime)
        
        start_str = current_start.isoformat() + "Z"
        end_str = current_end.isoformat() + "Z"
        
        query_url = f"{base_url}/query_range?query={urllib.parse.quote(query)}&start={start_str}&end={end_str}&step=1h"
        
        app.logger.debug(f"Querying VictoriaMetrics chunk from {start_str} to {end_str}")
        
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
                        all_data.append(df_result)
                        
        except Exception as e:
            app.logger.error(f'Error processing chunk: {str(e)}')
            pass 

        current_start = current_end

    if not all_data:
        return pd.DataFrame(columns=['station', 'date'] + selected_cols)

    final_df = pd.concat(all_data, ignore_index=True)
    
    for col in selected_cols:
        if col not in final_df.columns:
            final_df[col] = np.nan
            
    return final_df

@app.route('/getdata')
def index():
    variables = request.args.getlist('variables') or selected_cols
    
    now = datetime.datetime.now()
    one_week_ago = now - datetime.timedelta(days=7)
    
    start_date = request.args.get('start_date', one_week_ago.strftime('%Y-%m-%d'))
    start_time = request.args.get('start_time', '00:00')
    end_date = request.args.get('end_date', now.strftime('%Y-%m-%d'))
    end_time = request.args.get('end_time', '00:00')
    
    station_filter = request.args.get('station_filter', '')

    return render_template_string('''
        <!DOCTYPE html>
        <html>
        <head>
            <title>API AireCiudadano</title>
            <style>
                body { font-family: Arial, sans-serif; max-width: 600px; margin: 20px auto; }
                .alert { padding: 15px; margin-bottom: 20px; border: 1px solid transparent; border-radius: 4px; display: none; }
                .alert-info { color: #31708f; background-color: #d9edf7; border-color: #bce8f1; display: block; }
                #status_message { margin-top: 15px; font-size: 16px; }
            </style>
        </head>
        <body>
            <form id="dataForm" action="/dataresult" method="post">
                <h2>API AIRECIUDADANO v2.0 (VM)</h2>
                <div class="alert alert-info">
                    <strong>New!</strong> You can now download up to 1 year of data instantly using the CSV format.
                </div>
                
                <label><b>Select variables:</b></label><br><br>
                {% for col in selected_cols %}
                    <input type="checkbox" id="{{ col }}" name="variables" value="{{ col }}" {% if col in variables %}checked{% endif %}>
                    <label for="{{ col }}">{{ col }}</label><br>
                {% endfor %}
                <br>
                
                <label>Start date/time:</label>
                <input type="date" name="start_date" value="{{ start_date }}" required>
                <input type="time" name="start_time" value="{{ start_time }}" step="3600" required><br><br>
                
                <label>End date/time:</label>
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
                    
                    // If returning to screen, let standard HTML form behavior handle it
                    if (format === 'screen') {
                        statusDiv.innerHTML = '<span style="color: blue;"><b>Processing your request...</b></span>';
                        return true; 
                    }

                    // For file downloads, intercept to show exact status
                    event.preventDefault();
                    statusDiv.innerHTML = '<span style="color: blue;"><b>⏳ Processing your request, please wait... This may take a moment.</b></span>';
                    btn.disabled = true;
                    btn.style.background = '#ccc';

                    fetch('/dataresult', {
                        method: 'POST',
                        body: new FormData(event.target)
                    }).then(async response => {
                        btn.disabled = false;
                        btn.style.background = '#007bff';
                        
                        const contentType = response.headers.get('content-type');
                        
                        // If response is JSON, it means an error occurred
                        if (contentType && contentType.includes('application/json')) {
                            const data = await response.json();
                            if (data.error) {
                                statusDiv.innerHTML = '<span style="color: red;"><b>❌ Error:</b> ' + data.error + '</span>';
                            } else if (data.message) {
                                statusDiv.innerHTML = '<span style="color: orange;"><b>⚠️ Notice:</b> ' + data.message + '</span>';
                            }
                        } else {
                            // If response is ZIP file, process the download
                            const blob = await response.blob();
                            const url = window.URL.createObjectURL(blob);
                            const a = document.createElement('a');
                            a.href = url;
                            
                            // Try to read original filename, otherwise create one
                            let filename = 'data_export.zip';
                            const disp = response.headers.get('Content-Disposition');
                            if (disp && disp.includes('filename=')) {
                                filename = disp.split('filename=')[1].replace(/"/g, '');
                            }
                            
                            a.download = filename;
                            document.body.appendChild(a);
                            a.click();
                            a.remove();
                            window.URL.revokeObjectURL(url);
                            
                            statusDiv.innerHTML = '<span style="color: green;"><b>✅ Success! The file has been downloaded automatically.</b></span>';
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

        obs = fetch_vm_data(base_url, query, variables, start_datetime, end_datetime)

        if obs.empty:
            processing_lock.release()
            return jsonify({'message': 'No data found for the selected period and stations.'})

        obs['date'] = pd.to_datetime(obs['date'], utc=True)
        obs = obs.sort_values(by=['station', 'date'])

        for col in variables:
            if col in obs.columns:
                obs[col] = pd.to_numeric(obs[col], errors='coerce').round(3)

        total_records = obs.shape[0]
        obs['date'] = obs['date'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

        # Fast NaN replacement
        obs = obs.replace({np.nan: None})

        process_duration = time.time() - start_time_proc
        hours, remainder = divmod(int(process_duration), 3600)
        minutes, seconds = divmod(remainder, 60)
        formatted_duration = f"{hours}:{minutes:02}:{seconds:02}"

        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'data_{start_date}_{end_date}_{timestamp}.zip'

        if result_format == 'filecsv':
            memory_file = io.BytesIO()
            with zipfile.ZipFile(memory_file, 'w', zipfile.ZIP_DEFLATED) as zf:
                csv_buffer = io.StringIO()
                obs.to_csv(csv_buffer, index=False)
                zf.writestr('data.csv', csv_buffer.getvalue())
            memory_file.seek(0)
            
            processing_lock.release()
            return Response(
                memory_file.getvalue(),
                mimetype='application/zip',
                headers={
                    'Content-Disposition': f'attachment; filename="{filename}"',
                    'Access-Control-Expose-Headers': 'Content-Disposition'
                }
            )

        # Dictionary build only runs if result_format is 'screen' or 'filejson'
        grouped_data = {
            station: group.drop(columns=['station']).to_dict(orient='records') 
            for station, group in obs.groupby('station')
        }

        result_data = {
            'total_records': total_records,
            'data': grouped_data,
            'process_duration': formatted_duration
        }

        if result_format == 'screen':
            processing_lock.release()
            return jsonify(result_data)
        elif result_format == 'filejson':
            memory_file = io.BytesIO()
            with zipfile.ZipFile(memory_file, 'w', zipfile.ZIP_DEFLATED) as zf:
                data_str = json.dumps(result_data, indent=2)
                zf.writestr('data.json', data_str)
            memory_file.seek(0)
            
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