# Para servidor local sin limites VM

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

# 1. Nuevas columnas con sufijo _1h
selected_cols = [
    "PM25_1h", "PM25raw_1h", "PM1_1h", "Humidity_1h", "Temperature_1h",
]

# Initialize Flask app and set logging level
app = Flask(__name__)
app.logger.setLevel(logging.DEBUG)

# Global lock for one-at-a-time processing
processing_lock = threading.Lock()

def create_zip_file(data, file_format='json'):
    memory_file = io.BytesIO()
    try:
        with zipfile.ZipFile(memory_file, 'w', zipfile.ZIP_DEFLATED) as zf:
            if file_format == 'json':
                data_str = json.dumps(data, indent=2)
                zf.writestr('data.json', data_str)
            else:  # csv
                rows = []
                for station, records in data['data'].items():
                    for record in records:
                        record['station'] = station
                        rows.append(record)
                df = pd.DataFrame(rows)
                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, index=False)
                zf.writestr('data.csv', csv_buffer.getvalue())

        memory_file.seek(0)
        return memory_file
    except Exception as e:
        app.logger.error(f'Error creating zip file: {str(e)}')
        raise

# Nueva función simple sin chunks
def fetch_vm_data(base_url, query, selected_cols, start_datetime, end_datetime):
    start_str = start_datetime.isoformat() + "Z"
    end_str = end_datetime.isoformat() + "Z"
    
    # Se añade step=1h nativo a la consulta
    query_url = f"{base_url}/query_range?query={urllib.parse.quote(query)}&start={start_str}&end={end_str}&step=1h"
    
    app.logger.debug(f"Querying VictoriaMetrics from {start_str} to {end_str}")
    
    response = requests.get(query_url)
    response.raise_for_status()
    data = response.json().get('data', {}).get('result', [])

    if not data:
        return pd.DataFrame(columns=['station', 'date'] + selected_cols)

    df = pd.json_normalize(data)
    
    # VictoriaMetrics limpio usa metric.job para la estación
    if 'metric.job' not in df.columns:
        return pd.DataFrame(columns=['station', 'date'] + selected_cols)

    if 'values' in df.columns:
        df = df.explode('values')
        df['date'] = df['values'].apply(lambda x: datetime.datetime.fromtimestamp(x[0], tz=datetime.timezone.utc).isoformat())
        df['value'] = df['values'].apply(lambda x: float(x[1]))
        df = df.drop(columns="values")

    # Renombramos usando el 'job' directo
    df = df.rename(columns={"metric.__name__": "metric_name", "metric.job": "station"})
    df = df[df['station'].notnull()]

    if df.empty:
        return pd.DataFrame(columns=['station', 'date'] + selected_cols)

    # Pivot table a formato ancho
    df_result = pd.pivot(df, index=['station', 'date'], columns='metric_name', values='value').reset_index()
    
    # Asegurar que todas las columnas existan, incluso si vienen vacías
    for col in selected_cols:
        if col not in df_result.columns:
            df_result[col] = np.nan
            
    return df_result


@app.route('/getdata')
def index():
    variables = request.args.getlist('variables') or selected_cols
    start_date = request.args.get('start_date', '2024-05-09')
    start_time = request.args.get('start_time', '08:00')
    end_date = request.args.get('end_date', '2024-05-09')
    end_time = request.args.get('end_time', '10:00')
    station_filter = request.args.get('station_filter', '')

    return render_template_string('''
        <form action="/dataresult" method="post">
            <h2>API AIRECIUDADANO v2.0 (VictoriaMetrics)</h2>
            <label for="variables">Select variables:</label><br><br>
            {% for col in selected_cols %}
                <input type="checkbox" id="{{ col }}" name="variables" value="{{ col }}" {% if col in variables %}checked{% endif %}>
                <label for="{{ col }}">{{ col }}</label><br>
            {% endfor %}
            <br>
            <label for="start_date">Start date/time:</label>
            <input type="date" id="start_date" name="start_date" value="{{ start_date }}">
            <label for="start_time"> / </label>
            <input type="time" id="start_time" name="start_time" value="{{ start_time }}" step="3600"><br><br>
            <label for="end_date">End date/time:</label>
            <input type="date" id="end_date" name="end_date" value="{{ end_date }}">
            <label for="end_time"> / </label>
            <input type="time" id="end_time" name="end_time" value="{{ end_time }}" step="3600"><br><br>
            
            <label for="station_filter">Station Filter:</label>
            <input type="text" id="station_filter" name="station_filter" value=""><br><br>

            <label for="result_format">Result format:</label>
            <select id="result_format" name="result_format">
                <option value="screen">Result in screen</option>
                <option value="filejson">Result in json ZIP file</option>
                <option value="filecsv">Result in csv ZIP file</option>
            </select><br><br>

            <input type="submit" value="Submit">
        </form>
    ''', selected_cols=selected_cols, variables=variables, start_date=start_date,
       start_time=start_time, end_date=end_date, end_time=end_time)

@app.route('/dataresult', methods=['POST'])
def data():
    if not processing_lock.acquire(blocking=False):
        return jsonify({
            'error': 'The API is currently processing another request. Please wait and try again shortly.'
        })

    start_time_proc = time.time()
    try:
        variables = request.form.getlist('variables')
        # 3. Nueva URL base apuntando al dominio de producción con el puerto 30001
        base_url = "http://sensor.aireciudadano.com:30001/api/v1"

        start_date = request.form['start_date']
        start_time_str = request.form['start_time']
        end_date = request.form['end_date']
        end_time = request.form['end_time']
        station_filter = request.form.get('station_filter', '')
        result_format = request.form.get('result_format', 'screen')

        start_datetime = datetime.datetime.fromisoformat(f"{start_date}T{start_time_str}")
        end_datetime = datetime.datetime.fromisoformat(f"{end_date}T{end_time}")
        
        # 2. Límite estricto de 1 año (366 días considerando bisiestos)
        date_diff = end_datetime - start_datetime
        if date_diff.days > 366:
            processing_lock.release()
            return jsonify({
                'error': 'El cálculo máximo permitido es de 1 año completo (365 días). Por favor reduce el rango de fechas.'
            })

        # Construcción de la consulta PromQL
        metrics_regex = "|".join(variables)
        if station_filter:
            station_regex = station_filter.replace(',', '|')
            query = f'{{__name__=~"{metrics_regex}", job=~".*({station_regex}).*"}}'
        else:
            query = f'{{__name__=~"{metrics_regex}"}}'

        # Fetch directo sin chunks
        obs = fetch_vm_data(base_url, query, variables, start_datetime, end_datetime)

        if obs.empty:
            processing_lock.release()
            return jsonify({'message': 'No data found for the selected period.'})

        # Dar formato exacto al viejo script para que la comparación sea impecable
        obs['date'] = pd.to_datetime(obs['date'], utc=True)
        obs = obs.sort_values(by=['station', 'date'])

        # Redondear a 3 decimales
        for col in variables:
            if col in obs.columns:
                obs[col] = pd.to_numeric(obs[col], errors='coerce').round(3)

        total_records = obs.shape[0]
        obs['date'] = obs['date'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        json_data = obs.to_dict(orient='records')

        # Limpiar NaNs
        for record in json_data:
            for key, value in record.items():
                if pd.isna(value):
                    record[key] = None

        # Agrupar por estación
        grouped_data = {}
        for record in json_data:
            station = record.pop('station')
            if station not in grouped_data:
                grouped_data[station] = []
            grouped_data[station].append(record)

        process_duration = time.time() - start_time_proc
        hours, remainder = divmod(int(process_duration), 3600)
        minutes, seconds = divmod(remainder, 60)
        formatted_duration = f"{hours}:{minutes:02}:{seconds:02}"

        result_data = {
            'total_records': total_records,
            'data': grouped_data,
            'process_duration': formatted_duration
        }

        processing_lock.release()

        if result_format == 'screen':
            return jsonify(result_data)
        else:
            try:
                memory_file = create_zip_file(result_data, 'json' if result_format == 'filejson' else 'csv')
                timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f'data_{start_date}_{end_date}_{timestamp}.zip'
                
                return Response(
                    memory_file.getvalue(),
                    mimetype='application/zip',
                    headers={
                        'Content-Disposition': f'attachment; filename={filename}',
                        'Content-Type': 'application/zip'
                    }
                )
            except Exception as e:
                return jsonify({'error': f'Error creating download file: {str(e)}'})

    except Exception as e:
        if processing_lock.locked():
            processing_lock.release()
        app.logger.error(f'Error in data endpoint: {str(e)}')
        return jsonify({'error': str(e)})

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8084)