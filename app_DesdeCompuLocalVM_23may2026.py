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

# Nueva función con "chunks" ultrarrápidos para evitar el límite 422 de VictoriaMetrics
def fetch_vm_data(base_url, query, selected_cols, start_datetime, end_datetime):
    chunk_size = datetime.timedelta(days=90) # Pedimos en bocados de 3 meses máximo
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
            
            # Si ocurre el error 422, imprimimos el mensaje real de la base de datos
            if response.status_code == 422:
                app.logger.error(f"VM Limit Error Detail: {response.text}")
                
            response.raise_for_status()
            data = response.json().get('data', {}).get('result', [])

            if data:
                df = pd.json_normalize(data)
                
                # Verificamos que tenga la columna de la estación
                if 'metric.job' in df.columns and 'values' in df.columns:
                    df = df.explode('values')
                    df['date'] = df['values'].apply(lambda x: datetime.datetime.fromtimestamp(x[0], tz=datetime.timezone.utc).isoformat())
                    df['value'] = df['values'].apply(lambda x: float(x[1]))
                    df = df.drop(columns="values")

                    df = df.rename(columns={"metric.__name__": "metric_name", "metric.job": "station"})
                    df = df[df['station'].notnull()]

                    if not df.empty:
                        # Usamos pivot_table con aggfunc='mean' para promediar cualquier duplicado en caso de choque de sensores
                        df_result = pd.pivot_table(df, index=['station', 'date'], columns='metric_name', values='value', aggfunc='mean').reset_index()
                        all_data.append(df_result)
                        
        except Exception as e:
            app.logger.error(f'Error processing chunk: {str(e)}')
            # En lugar de romper todo, saltamos al siguiente chunk
            pass 

        current_start = current_end

    # Si no obtuvimos ningún dato de ningún chunk, devolvemos DataFrame vacío
    if not all_data:
        return pd.DataFrame(columns=['station', 'date'] + selected_cols)

    # Pegamos todos los bloques de 3 meses en una sola tabla gigante
    final_df = pd.concat(all_data, ignore_index=True)
    
    # Aseguramos que todas las columnas existan
    for col in selected_cols:
        if col not in final_df.columns:
            final_df[col] = np.nan
            
    return final_df

@app.route('/getdata')
def index():
    variables = request.args.getlist('variables') or selected_cols
# 1. Calculamos las fechas dinámicas (Hoy y hace 7 días)
    now = datetime.datetime.now()
    one_week_ago = now - datetime.timedelta(days=7)
    
    # 2. Asignamos los valores por defecto calculados
    start_date = request.args.get('start_date', one_week_ago.strftime('%Y-%m-%d'))
    start_time = request.args.get('start_time', '00:00') # Comienza a la medianoche de hace 7 días
    end_date = request.args.get('end_date', now.strftime('%Y-%m-%d'))
    end_time = request.args.get('end_time', '00:00')     # Termina a las 23:00 de hoy
    
    station_filter = request.args.get('station_filter', '')

    return render_template_string('''
        <form action="/dataresult" method="post">
            <h2>API AIRECIUDADANO v2.0 (VM)</h2>
            <h3>Instructions at: <a href="https://aireciudadano.com/apidatavm/" target="_blank">aireciudadano.com/apidata</a></h3><br>
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
        
        if date_diff.days > 7 and result_format == 'screen':
            return jsonify({
            'error': 'For time ranges longer than 7 days, please select JSON or CSV file format'
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