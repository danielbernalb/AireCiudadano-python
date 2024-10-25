from flask import Flask, request, jsonify, render_template_string, send_file
import requests
import datetime
import json
import os
import zipfile
import tempfile
from werkzeug.utils import secure_filename
import logging
from io import StringIO
import csv

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
            current_start_time_adjusted = current_start_time + datetime.timedelta(seconds=60)
            current_end_time = min(current_start_time + datetime.timedelta(seconds=interval_seconds), end_datetime)
            query_url = f"{url}&start={current_start_time_adjusted.isoformat()}Z&end={current_end_time.isoformat()}Z&step={step}"

            app.logger.debug(f"Fd: {query_url}")

            try:
                response = requests.get(query_url)
                response.raise_for_status()
                data = response.json()['data']['result']

                processed_data = []
                for item in data:
                    station = item['metric'].get('exported_job', 'unknown_station')
                    metric_name = item['metric'].get('__name__', '')

                    if 'values' in item:
                        for timestamp, value in item['values']:
                            date = datetime.datetime.utcfromtimestamp(timestamp).isoformat()
                            processed_data.append({
                                'station': station,
                                'date': date,
                                metric_name: float(value)
                            })
                    elif 'value' in item:
                        timestamp, value = item['value']
                        date = datetime.datetime.utcfromtimestamp(timestamp).isoformat()
                        processed_data.append({
                            'station': station,
                            'date': date,
                            metric_name: float(value)
                        })

                yield processed_data

            except Exception as e:
                app.logger.error(f'Error fetching data chunk: {str(e)}')
                raise

            current_start_time = current_end_time

    all_data = []
    for chunk in data_generator():
        all_data.extend(chunk)

    return all_data

# Constructor of the step value for time range queries
def _get_step(number, choice):
    options = {"minutes": "m", "hours": "h", "days": "d", "weeks": "w", "years": "y"}
    return f"{number}{options[choice]}"

# Function to get wide table
def _wide_table(df, selected_cols):
    try:
        if 'station' not in df.columns:
            df['station'] = 'unknown_station'
            app.logger.warning("'station' column not found, using 'unknown_station' as default")

        df['value'] = pd.to_numeric(df['value'], errors='coerce')

        duplicates = df.duplicated(subset=['station', 'date'], keep=False)
        if duplicates.any():
            app.logger.warning(f"Found {duplicates.sum()} duplicates in 'station' and 'date' columns. Removing duplicates.")
            df = df.groupby(['station', 'date', 'metric_name'])['value'].mean().reset_index()

        df_result = pd.pivot(df, index=['station', 'date'], columns='metric_name', values='value').reset_index()

        all_cols = ['station', 'date'] + selected_cols
        missing_cols = set(all_cols) - set(df_result.columns)
        for col in missing_cols:
            df_result[col] = np.nan

        df_result = df_result[all_cols].reset_index(drop=True)
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
                <option value="filejson">Result in json-zip file</option>
                <option value="filexlsx">Result in xlsx-zip file</option>
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
        # Calcular la diferencia en días
        date_difference = (end_datetime - start_datetime).days

        if date_difference > 7:
            # Procesar en bloques de 7 días
            with tempfile.TemporaryDirectory() as temp_dir:
                current_start = start_datetime
                file_paths = []

                while current_start < end_datetime:
                    current_end = min(current_start + datetime.timedelta(days=7), end_datetime)

                    # Obtener datos para el bloque actual
                    if aggregation_method == 'average':
                        interval_seconds = 3600
                        obs = get_data(url, variables, current_start - datetime.timedelta(hours=1), current_end, step, interval_seconds)
                    else:
                        interval_seconds = 3600
                        obs = get_data(url, variables, current_start, current_end, step, interval_seconds)

                    if station_filter:
                        filters = station_filter.split(',')
                        obs = [record for record in obs if any(f.lower() in record['station'].lower() for f in filters)]

                    if aggregation_method == 'step':
                        obs = [record for record in obs if current_start <= datetime.datetime.fromisoformat(record['date']) <= current_end]
                    elif aggregation_method == 'average':
                        # Implementar la lógica de promedio horario aquí si es necesario
                        pass

                    # Guardar el bloque en un archivo temporal
                    if result_format == "filejson":
                        file_path = os.path.join(temp_dir, f"block_{current_start.strftime('%Y%m%d')}_{current_end.strftime('%Y%m%d')}.json")
                        with open(file_path, 'w') as f:
                            json.dump(obs, f)
                    elif result_format == "filexlsx":
                        file_path = os.path.join(temp_dir, f"block_{current_start.strftime('%Y%m%d')}_{current_end.strftime('%Y%m%d')}.csv")
                        with open(file_path, 'w', newline='') as f:
                            writer = csv.DictWriter(f, fieldnames=['station', 'date'] + variables)
                            writer.writeheader()
                            writer.writerows(obs)

                    file_paths.append(file_path)
                    current_start = current_end + datetime.timedelta(seconds=1)

                # Comprimir todos los archivos en un ZIP
                zip_filename = 'dataresult.zip'
                zip_path = os.path.join(temp_dir, zip_filename)
                with zipfile.ZipFile(zip_path, 'w') as zipf:
                    for file in file_paths:
                        zipf.write(file, os.path.basename(file))

                # Eliminar archivos temporales
                for file in file_paths:
                    os.remove(file)

                # Enviar archivo ZIP
                return send_file(zip_path, as_attachment=True, download_name=zip_filename)

        else:
            # Procesar normalmente para rangos de 7 días o menos
            if aggregation_method == 'average':
                interval_seconds=3600
                obs = get_data(url, variables, start_datetime_adjusted, end_datetime, step, interval_seconds)
            else:
                interval_seconds=3600
                obs = get_data(url, variables, start_datetime, end_datetime, step, interval_seconds)

            app.logger.info(f"Data fetched. Total records: {len(obs)}") 

            if station_filter:
                filters = station_filter.split(',')
                obs = [record for record in obs if any(f.lower() in record['station'].lower() for f in filters)]

            if aggregation_method == 'step':
                obs = [record for record in obs if start_datetime <= datetime.datetime.fromisoformat(record['date']) <= end_datetime]
            elif aggregation_method == 'average':
                # Implementar la lógica de promedio horario aquí si es necesario
                pass

            if result_format == "screen":
                grouped_data = {}
                for record in obs:
                    station = record['station']
                    if station not in grouped_data:
                        grouped_data[station] = []
                    grouped_data[station].append({k: v for k, v in record.items() if k != 'station'})

                return jsonify({
                    'total_records': len(obs),
                    'data': grouped_data
                })

            elif result_format == "filejson":
                with tempfile.TemporaryDirectory() as temp_dir:
                    json_filename = secure_filename('dataresult.json')
                    zip_filename = secure_filename('dataresult.zip')
                    json_path = os.path.join(temp_dir, json_filename)
                    zip_path = os.path.join(temp_dir, zip_filename)
                    with open(json_path, 'w') as json_file:
                        json.dump(obs, json_file, indent=4)
                    with zipfile.ZipFile(zip_path, 'w') as zipf:
                        zipf.write(json_path, json_filename, compress_type=zipfile.ZIP_DEFLATED)
                    return send_file(zip_path, as_attachment=True, download_name=zip_filename)

            elif result_format == "filexlsx":
                with tempfile.TemporaryDirectory() as temp_dir:
                    csv_filename = secure_filename('dataresult.csv')
                    csv_path = os.path.join(temp_dir, csv_filename)
                    with open(csv_path, 'w', newline='') as csv_file:
                        writer = csv.DictWriter(csv_file, fieldnames=['station', 'date'] + variables)
                        writer.writeheader()
                        writer.writerows(obs)
                    return send_file(csv_path, as_attachment=True, download_name=csv_filename)

    except PermissionError as e:
        app.logger.error(f'Permission error: {str(e)}')
        return jsonify({'error': 'No se pudo guardar el archivo debido a permisos insuficientes'}), 403
    except Exception as e:
        app.logger.error(f'Error in data endpoint: {str(e)}', exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.errorhandler(Exception)
def handle_exception(e):
    app.logger.error(f'Unhandled exception: {str(e)}', exc_info=True)

    if isinstance(e, requests.exceptions.RequestException):
        error_message = "Error al obtener datos de la API externa. Por favor, inténtelo de nuevo más tarde."
    elif isinstance(e, pd.errors.EmptyDataError):
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