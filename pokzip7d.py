from flask import Flask, request, jsonify, render_template_string, send_file
import requests
import dask
import datetime
from datetime import timedelta  # Importación directa de datetime y timedelta
import pandas as pd
import dask.dataframe as dd
import numpy as np
import json
import os
import zipfile
import time
import tempfile
from werkzeug.utils import secure_filename
import logging
from io import StringIO

# Deshabilitar PyArrow para evitar el error con `StringDtype`
dask.config.set({"dataframe.convert-string": False})

# Constants
selected_cols = [
    "PM25", "PM25raw", "PM1", "Humidity", "Temperature",
]

# Flask application
app = Flask(__name__)
app.logger.setLevel(logging.DEBUG) 

# Get data from API with time intervals
def get_data(url, selected_cols, start_datetime, end_datetime, step, interval_seconds):
    def data_generator():
        current_start_time = start_datetime
        while current_start_time < end_datetime:
            current_end_time = min(current_start_time + datetime.timedelta(seconds=interval_seconds), end_datetime)
            query_url = f"{url}&start={current_start_time.isoformat()}Z&end={current_end_time.isoformat()}Z&step={step}"

            app.logger.debug(f"Fetching data from: {query_url}")

            try:
                response = requests.get(query_url, timeout=30)
                response.raise_for_status()
                data = response.json()['data']['result']
                df = dd.from_pandas(pd.json_normalize(data), npartitions=4)

                app.logger.debug(f"Data fetched. Shape: {df.shape}")

                if 'values' in df.columns:
                    # Conviértelo temporalmente a Pandas solo para ejecutar explode
                    df = df.map_partitions(lambda df: df.explode('values'), meta=df)  # Usar `map_partitions` para trabajar con Pandas dentro de Dask
                    df['date'] = df['values'].apply(lambda x: datetime.datetime.utcfromtimestamp(x[0]).isoformat(), meta=('date', str))
                    df['value'] = df['values'].apply(lambda x: x[1], meta=('value', float))
                    df = df.drop(columns="values")
                elif 'value' in df.columns:
                    df['date'] = df['value'].apply(lambda x: datetime.datetime.utcfromtimestamp(x[0]).isoformat(), meta=('date', str))
                    df['value'] = df['value'].apply(lambda x: x[1], meta=('value', float))

                rename_dict = {
                    "metric.__name__": "metric_name",
                    "metric.exported_job": "station",
                }
                df = df.rename(columns={k: v for k, v in rename_dict.items() if k in df.columns})

                df = df.drop(columns=[col for col in df.columns if 'metric.' in col], errors='ignore')

                if 'station' in df.columns:
                    df = df[df['station'].notnull()]
                else:
                    app.logger.warning("'station' column not found in the data")

                df_result = _wide_table(df, selected_cols)

                for col in selected_cols:
                    if col in df_result.columns:
                        df_result[col] = df_result[col].astype(float)
                if 'Latitude' in df_result.columns:
                    df_result['Latitude'].replace(0, np.nan, inplace=True)
                if 'Longitude' in df_result.columns:
                    df_result['Longitude'].replace(0, np.nan, inplace=True)

                yield df_result.compute()

            except Exception as e:
                app.logger.error(f'Error fetching data chunk: {str(e)}')
                raise

            current_start_time = current_end_time

    return dd.concat([df for df in data_generator()]).drop_duplicates(subset=['date', 'station']).compute()

# Mover la función fuera de _wide_table

def remove_duplicates_partition(partition):
    """
    Elimina duplicados en cada partición por 'station' y 'date'.
    """
    duplicates = partition.duplicated(subset=['station', 'date'], keep=False)
    if duplicates.any():
        app.logger.warning(f"Found duplicates in 'station' and 'date' columns. Removing duplicates.")
        partition = partition.groupby(['station', 'date', 'metric_name'])['value'].mean().reset_index()
    return partition

def _wide_table(df, selected_cols):
    try:
        if 'station' not in df.columns:
            df['station'] = 'unknown_station'
            app.logger.warning("'station' column not found, using 'unknown_station' as default")

        # Convertimos el tipo de 'value' a numérico
        df['value'] = dd.to_numeric(df['value'], errors='coerce')

        # Forzar todas las columnas de texto a `object` antes de cualquier operación crítica
        for col in df.columns:
            if pd.api.types.is_string_dtype(df[col]):
                df[col] = df[col].astype('object')

        # Eliminar duplicados en cada partición usando la función global
        df = df.map_partitions(remove_duplicates_partition)

        # Convertir a Pandas temporalmente para usar pivot_table
        df = df.compute()  # Consolidamos el DataFrame en un solo DataFrame de Pandas
        df_result = df.pivot_table(index=['station', 'date'], columns='metric_name', values='value').reset_index()

        all_cols = ['station', 'date'] + selected_cols
        missing_cols = set(all_cols) - set(df_result.columns)
        for col in missing_cols:
            df_result[col] = np.nan

        # Convertir de vuelta a Dask para procesar en paralelo si es necesario
        df_result = dd.from_pandas(df_result[all_cols], npartitions=4)
        df_result.columns.name = ""
        return df_result

    except Exception as e:
        app.logger.error(f'Pivot Error: {str(e)}')
        raise

# Constructor of the step value for time range queries
def _get_step(number, choice):
    options = {"minutes": "m", "hours": "h", "days": "d", "weeks": "w", "years": "y"}
    return f"{number}{options[choice]}"

@app.route('/getdata')
def index():
    variables = request.args.getlist('variables') or selected_cols
    start_date = request.args.get('start_date', '2024-05-09')
    start_time = request.args.get('start_time', '08:00')
    end_date = request.args.get('end_date', '2024-05-09')
    end_time = request.args.get('end_time', '10:00')
    step_number = request.args.get('step_number', '1')
    step_option = request.args.get('step_option', 'hours')
    station_filter = request.args.get('station_filter', '')
    aggregation_method = request.args.get('aggregation_method', 'step')

    return render_template_string('''
        <form action="/dataresult" method="post">
            <h2>API AIRECIUDADANO v1.0</h2>
            <h3>Instructions at: <a href="https://aireciudadano.com/apidata/" target="_blank">aireciudadano.com/apidata</a></h3><br>
            <label for="variables">Select variables:</label><br>
            <br>
            {% for col in selected_cols %}
                <input type="checkbox" id="{{ col }}" name="variables" value="{{ col }}" {% if col in variables %}checked{% endif %}>
                <label for="{{ col }}">{{ col }}</label><br>
            {% endfor %}
            <br>
            <label for="start_date">Start date/time:</label>
            <input type="date" id="start_date" name="start_date" value="{{ start_date }}">
            <label for="start_time"> / </label>
            <input type="time" id="start_time" name="start_time" value="{{ start_time }}" step="3600" list="hour-markers" required><br><br>
            <label for="end_date">End date/time:</label>
            <input type="date" id="end_date" name="end_date" value="{{ end_date }}">
            <label for="end_time"> / </label>
            <input type="time" id="end_time" name="end_time" value="{{ end_time }}" step="3600" list="hour-markers" required><br><br>
            <datalist id="hour-markers">
                {% for hour in range(24) %}
                    <option value="{{ '%02d:00'|format(hour) }}"></option>
                {% endfor %}
            </datalist>
            <label for="aggregation_method">Aggregation method:</label>
            <select id="aggregation_method" name="aggregation_method">
                <option value="step" {% if aggregation_method == 'step' %}selected{% endif %}>Step</option>
                <option value="average" {% if aggregation_method == 'average' %}selected{% endif %}>Average</option>
            </select><br><br>
            <label for="step_number">Step/Average number:</label>
            <input type="number" id="step_number" name="step_number" value="{{ step_number }}">
            <label for="step_option">Option:</label>
            <select id="step_option" name="step_option">
                <option value="minutes" {% if step_option == 'minutes' %}selected{% endif %}>Minutes</option>
                <option value="hours" {% if step_option == 'hours' %}selected{% endif %}>Hours</option>
                <option value="days" {% if step_option == 'days' %}selected{% endif %}>Days</option>
                <option value="weeks" {% if step_option == 'weeks' %}selected{% endif %}>Weeks</option>
            </select><br><br>
            <label for="station_filter">Station Filter:</label>
            <input type="text" id="station_filter" name="station_filter" value=""><br><br>
            <label for="result_format">Result format:</label>
            <select id="result_format" name="result_format">
                <option value="screen">Result in screen</option>
                <option value="filejson">Result in json ZIP file</option>
                <option value="filecsv">Result in csv file</option>
            </select><br><br>
            <input type="submit" value="Submit">
        </form>
        <script>
            function toggle(source) {
                checkboxes = document.getElementsByName('variables');
                for (var i = 0; i < checkboxes.length; i++) {
                    checkboxes[i].checked = source.checked;
                }
            }
        </script>
    ''', selected_cols=selected_cols, variables=variables, start_date=start_date, start_time=start_time,
       end_date=end_date, end_time=end_time, step_number=step_number, step_option=step_option, aggregation_method=aggregation_method)

@app.route('/dataresult', methods=['POST'])
def data():
    variables = request.form.getlist('variables')
    base_url = "http://sensor.aireciudadano.com:30000/api/v1"
    query = '{job%3D"pushgateway"}'

    start_date = request.form['start_date']
    start_time = request.form['start_time']
    end_date = request.form['end_date']
    end_time = request.form['end_time']
    step_number = request.form['step_number']
    step_option = request.form['step_option']
    aggregation_method = request.form['aggregation_method']
    result_format = request.form['result_format']
    station_filter = request.form.get('station_filter', '')

    start_datetime = datetime.datetime.fromisoformat(f"{start_date}T{start_time}")
    start_datetime_adjusted = start_datetime - datetime.timedelta(hours=1)
    end_datetime = datetime.datetime.fromisoformat(f"{end_date}T{end_time}")

    app.logger.info(f"Processing request: {start_datetime} to {end_datetime}") 

    if aggregation_method == 'average':
        step = '1m'
    else:
        step = _get_step(step_number, step_option)

    url = f"{base_url}/query_range?query={query}"

    try:
        time_range = end_datetime - start_datetime
        if time_range.days > 7:
            block_size = timedelta(days=7)  # Usar `timedelta` directamente
            current_start = start_datetime
            data_blocks = []

            while current_start < end_datetime:
                current_end = min(current_start + block_size, end_datetime)

                app.logger.info(f"Processing block: {current_start} to {current_end}")

                interval_seconds = 3600
                block_data = get_data(url, variables, current_start, current_end, step, interval_seconds)

                if aggregation_method == 'step':
                    # Convertir la columna 'date' a datetime en paralelo
                    block_data['date'] = dd.to_datetime(block_data['date'], utc=True)
                    
                    # Aplicar las máscaras en paralelo
                    mask_start = block_data['date'] == pd.to_datetime(current_start, utc=True)
                    mask_step = (block_data['date'] > pd.to_datetime(current_start, utc=True)) & (block_data['date'] <= pd.to_datetime(current_end, utc=True))
                    
                    # Filtrar los datos usando las máscaras generadas
                    block_data = block_data[mask_start | mask_step]

                elif aggregation_method == 'average':
                    # Convertir la columna 'date' a datetime y establecer un índice compuesto
                    block_data['date'] = dd.to_datetime(block_data['date'], utc=True)
                    block_data = block_data.set_index(['station', 'date'])

                    # Convertir valores a numérico en cada partición usando `map_partitions`
                    block_data = block_data.map_partitions(lambda df: df.apply(pd.to_numeric, errors='coerce'))

                    hourly_obs = []

                    # Definir el tiempo de inicio y fin para los promedios horarios
                    current_time = current_start
                    while current_time <= current_end:
                        # Aplicar máscaras para el rango de tiempo
                        mask = (block_data.index.get_level_values('date') > current_time - timedelta(hours=1)) & \
                            (block_data.index.get_level_values('date') <= current_time)
                        
                        # Calcular el promedio de cada estación en el rango de tiempo
                        hourly_avg = block_data.loc[mask].groupby('station').mean().compute()
                        hourly_avg['station'] = hourly_avg.index
                        hourly_avg['date'] = current_time.strftime('%Y-%m-%dT%H:%M:%SZ')
                        
                        # Guardar el resultado en la lista de observaciones
                        hourly_obs.append(hourly_avg)
                        
                        # Incrementar el tiempo para la siguiente iteración
                        current_time += timedelta(hours=1)

                    # Convertir la lista de resultados en un DataFrame de Dask
                    block_data = dd.from_pandas(pd.DataFrame(hourly_obs), npartitions=4)

                # Agregar el bloque de datos procesado a la lista de bloques
                data_blocks.append(block_data)
                current_start = current_end

            # Concatenar todos los bloques de datos en un solo DataFrame de Dask
            obs = dd.concat(data_blocks, ignore_index=True).compute()
        else:
            # Procesar los datos normalmente si el rango es menor o igual a 15 días
            if aggregation_method == 'average':
                interval_seconds = 3600
                obs = get_data(url, variables, start_datetime_adjusted, end_datetime, step, interval_seconds)
            else:
                interval_seconds = 3600
                obs = get_data(url, variables, start_datetime, end_datetime, step, interval_seconds)

        app.logger.info(f"Data fetched. Shape: {obs.shape}") 

        if station_filter:
            filters = station_filter.split(',')
            obs = obs[obs['station'].str.contains('|'.join(filters), case=False)]

        if aggregation_method == 'step':
            obs['date'] = dd.to_datetime(obs['date'], utc=True)
            mask_start = obs['date'] == pd.to_datetime(start_datetime, utc=True)
            mask_step = (obs['date'] > pd.to_datetime(start_datetime, utc=True)) & (obs['date'] <= pd.to_datetime(end_datetime, utc=True))
            obs = obs[mask_start | mask_step]
        elif aggregation_method == 'average':
            obs['date'] = dd.to_datetime(obs['date'], utc=True)
            obs = obs.set_index(['station', 'date'])

            hourly_obs = []
            start_time_dt = pd.to_datetime(start_datetime, utc=True)
            end_time_dt = pd.to_datetime(end_datetime, utc=True)

            for station, group in obs.groupby('station'):
                current_time = start_time_dt
                while current_time <= end_time_dt:
                    mask = (group.index.get_level_values('date') > current_time - pd.Timedelta(hours=1)) & (group.index.get_level_values('date') <= current_time)
                    hourly_avg = group.loc[mask].mean()
                    hourly_avg['station'] = station
                    hourly_avg['date'] = current_time.strftime('%Y-%m-%dT%H:%M:%SZ')
                    hourly_obs.append(hourly_avg)
                    current_time += pd.Timedelta(hours=1)

            obs = dd.from_pandas(pd.DataFrame(hourly_obs), npartitions=4)

        if isinstance(obs, dd.DataFrame):
            obs = obs.compute()  # Consolidar el Dask DataFrame a Pandas

        # Ordenar el DataFrame por las columnas 'station' y 'date' para mantener el orden cronológico
        obs = obs.sort_values(by=['station', 'date']).reset_index(drop=True)

        total_records = obs.shape[0]

        app.logger.info(f"Total records: {total_records}")

        # Convertir columnas de tipo datetime a cadena de texto en formato ISO para serialización JSON
        if 'date' in obs.columns:
            obs['date'] = obs['date'].astype(str)

        # Convertir el DataFrame a diccionario para JSON
        json_data = obs.to_dict(orient='records')
        for record in json_data:
            for key, value in record.items():
                if pd.isna(value):
                    record[key] = None

        grouped_data = {}
        for record in json_data:
            station = record.pop('station')
            if station not in grouped_data:
                grouped_data[station] = []
            grouped_data[station].append(record)

        if result_format == "screen":
            return jsonify({
                'total_records': total_records,
                'data': grouped_data
            })

        elif result_format == "filejson":

            with tempfile.TemporaryDirectory() as temp_dir:
                json_filename = secure_filename('dataresult.json')
                zip_filename = secure_filename('dataresult.zip')

                json_path = os.path.join(temp_dir, json_filename)
                zip_path = os.path.join(temp_dir, zip_filename)

                # Guardar datos JSON en archivo temporal
                with open(json_path, 'w') as json_file:
                    json.dump(grouped_data, json_file, indent=4)

                # Comprimir archivo JSON a ZIP
                with zipfile.ZipFile(zip_path, 'w') as zipf:
                    zipf.write(json_path, json_filename, compress_type=zipfile.ZIP_DEFLATED)

                try:
                    os.remove(json_path)
                except Exception as e:
                    app.logger.warning(f"Could not delete temp JSON file: {str(e)}")

                # Enviar archivo ZIP
                return send_file(zip_path, as_attachment=True, download_name=zip_filename)

        elif result_format == "filecsv":

            with tempfile.TemporaryDirectory() as temp_dir:
                csv_filename = secure_filename('dataresult.csv')
                zip_filename = secure_filename('dataresult.zip')

                csv_path = os.path.join(temp_dir, csv_filename)
                zip_path = os.path.join(temp_dir, zip_filename)

                # Convertir a CSV
                obs.to_csv(csv_path, index=False)  # Ya es Pandas, así que no necesita `compute()`

                # Comprimir archivo CSV a ZIP
                with zipfile.ZipFile(zip_path, 'w') as zipf:
                    zipf.write(csv_path, csv_filename, compress_type=zipfile.ZIP_DEFLATED)

                try:
                    os.remove(csv_path)
                except Exception as e:
                    app.logger.warning(f"Could not delete temp CSV file: {str(e)}")

                # Enviar archivo ZIP
                return send_file(zip_path, as_attachment=True, download_name=zip_filename)

    except PermissionError as e:
        app.logger.error(f'Permission error: {str(e)}')
        return jsonify({'error': 'Permission denied'}), 403
    except Exception as e:
        app.logger.error(f'Error in data endpoint: {str(e)}', exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.errorhandler(Exception)
def handle_exception(e):
    # Registrar el error
    app.logger.error(f'Unhandled exception: {str(e)}', exc_info=True)

    # Preparar un mensaje de error más informativo
    if isinstance(e, requests.exceptions.RequestException):
        error_message = "Error al obtener datos de la API externa. Por favor, inténtelo de nuevo más tarde."
    elif isinstance(e, pd.errors.EmptyDataError):  # Manejo específico para errores de Pandas
        error_message = "No se encontraron datos para procesar. Por favor, verifique los parámetros de su solicitud."
    elif isinstance(e, PermissionError):
        error_message = "Error de permisos al intentar guardar el archivo. Por favor, contacte al administrador del sistema."
    elif isinstance(e, IOError):
        error_message = "Error al leer o escribir archivos. Por favor, verifique los permisos y el espacio en disco."
    else:
        error_message = "Ha ocurrido un error inesperado. Por favor, inténtelo de nuevo o contacte al soporte técnico."

    return jsonify({'error': error_message}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8081)