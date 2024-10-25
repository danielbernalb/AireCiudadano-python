from flask import Flask, request, jsonify, render_template_string, send_file
import requests
import datetime
import json
import os
import zipfile
import time
import tempfile
from werkzeug.utils import secure_filename
import logging
from collections import defaultdict
from datetime import timedelta
import csv

# Constants
selected_cols = [
    "PM25", "PM25raw", "PM1", "Humidity", "Temperature",
]

# Flask application
app = Flask(__name__)
app.logger.setLevel(logging.DEBUG)

def process_chunk_data(data, selected_cols):
    """Process raw API data without using pandas"""
    processed_data = defaultdict(list)
    
    for result in data['data']['result']:
        station = result.get('metric', {}).get('exported_job', 'unknown_station')
        metric_name = result.get('metric', {}).get('__name__')
        
        if metric_name not in selected_cols:
            continue
            
        for timestamp, value in result.get('values', []):
            date = datetime.datetime.utcfromtimestamp(timestamp).isoformat()
            try:
                value = float(value)
            except (ValueError, TypeError):
                value = None
                
            record = {
                'date': date,
                metric_name: value
            }
            processed_data[station].append((date, metric_name, value))
    
    # Consolidate measurements for same timestamps
    consolidated_data = defaultdict(lambda: defaultdict(dict))
    for station, measurements in processed_data.items():
        for date, metric, value in measurements:
            consolidated_data[station][date][metric] = value
    
    # Convert to list format
    final_data = []
    for station, dates in consolidated_data.items():
        for date, metrics in dates.items():
            record = {'station': station, 'date': date}
            for col in selected_cols:
                record[col] = metrics.get(col)
            final_data.append(record)
    
    return final_data

def get_data_chunk(url, selected_cols, start_time, end_time, step):
    query_url = f"{url}&start={start_time.isoformat()}Z&end={end_time.isoformat()}Z&step={step}"
    app.logger.debug(f"Fetching data from: {query_url}")
    
    response = requests.get(query_url)
    response.raise_for_status()
    data = response.json()
    
    return process_chunk_data(data, selected_cols)

def save_chunk_to_file(data, chunk_num, format_type, temp_dir):
    if format_type == "json":
        filename = f"chunk_{chunk_num}.json"
        filepath = os.path.join(temp_dir, filename)
        with open(filepath, 'w') as f:
            json.dump(data, f)
    else:  # xlsx (using csv as intermediate format)
        filename = f"chunk_{chunk_num}.csv"
        filepath = os.path.join(temp_dir, filename)
        if data:
            with open(filepath, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)
    return filepath

def create_final_file(temp_files, format_type, temp_dir):
    if format_type == "json":
        # Combine JSON files
        combined_data = defaultdict(list)
        for temp_file in temp_files:
            with open(temp_file, 'r') as f:
                chunk_data = json.load(f)
                for record in chunk_data:
                    combined_data[record['station']].append({
                        k: v for k, v in record.items() if k != 'station'
                    })
        
        final_json_path = os.path.join(temp_dir, 'final_data.json')
        with open(final_json_path, 'w') as f:
            json.dump(dict(combined_data), f)
        
        # Create ZIP file
        zip_path = os.path.join(temp_dir, 'dataresult.zip')
        with zipfile.ZipFile(zip_path, 'w') as zipf:
            zipf.write(final_json_path, 'dataresult.json')
        
        return zip_path
    
    else:  # xlsx
        # Combine CSV files into single CSV
        final_csv_path = os.path.join(temp_dir, 'dataresult.csv')
        with open(final_csv_path, 'w', newline='') as outfile:
            first_file = True
            for temp_file in temp_files:
                with open(temp_file, 'r') as infile:
                    if first_file:
                        outfile.write(infile.read())
                        first_file = False
                    else:
                        next(infile)  # Skip header
                        outfile.write(infile.read())
        
        return final_csv_path

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
                <option value="filexlsx">Result in xlsx file</option>
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
    try:
        variables = request.form.getlist('variables')
        base_url = "http://sensor.aireciudadano.com:30000/api/v1"
        query = '{job%3D"pushgateway"}'
        url = f"{base_url}/query_range?query={query}"

        # Parse input parameters
        start_datetime = datetime.datetime.fromisoformat(f"{request.form['start_date']}T{request.form['start_time']}")
        end_datetime = datetime.datetime.fromisoformat(f"{request.form['end_date']}T{request.form['end_time']}")
        step = _get_step(request.form['step_number'], request.form['step_option'])
        result_format = request.form['result_format']
        
        # Calculate time difference
        time_difference = end_datetime - start_datetime
        
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_files = []
            
            if time_difference.days > 7:
                # Process in 7-day chunks
                current_start = start_datetime
                chunk_num = 0
                
                while current_start < end_datetime:
                    chunk_end = min(current_start + timedelta(days=7), end_datetime)
                    
                    # Get and process chunk data
                    chunk_data = get_data_chunk(url, variables, current_start, chunk_end, step)
                    
                    # Save chunk to temporary file
                    format_type = "json" if result_format == "filejson" else "xlsx"
                    temp_file = save_chunk_to_file(chunk_data, chunk_num, format_type, temp_dir)
                    temp_files.append(temp_file)
                    
                    current_start = chunk_end
                    chunk_num += 1
                
                # Create final file
                final_path = create_final_file(temp_files, format_type, temp_dir)
                
                # Clean up temporary chunk files
                for temp_file in temp_files:
                    try:
                        os.remove(temp_file)
                    except Exception as e:
                        app.logger.warning(f"Could not remove temporary file {temp_file}: {str(e)}")
                
                if result_format == "filejson":
                    return send_file(final_path, as_attachment=True, download_name='dataresult.zip')
                else:
                    return send_file(final_path, as_attachment=True, download_name='dataresult.csv')
            
            else:
                # Process normally for periods <= 7 days
                data = get_data_chunk(url, variables, start_datetime, end_datetime, step)
                
                if result_format == "screen":
                    grouped_data = defaultdict(list)
                    for record in data:
                        station = record.pop('station')
                        grouped_data[station].append(record)
                    return jsonify({
                        'total_records': len(data),
                        'data': dict(grouped_data)
                    })
                
                elif result_format == "filejson":
                    json_path = os.path.join(temp_dir, 'dataresult.json')
                    zip_path = os.path.join(temp_dir, 'dataresult.zip')
                    
                    grouped_data = defaultdict(list)
                    for record in data:
                        station = record.pop('station')
                        grouped_data[station].append(record)
                    
                    with open(json_path, 'w') as f:
                        json.dump(dict(grouped_data), f)
                    
                    with zipfile.ZipFile(zip_path, 'w') as zipf:
                        zipf.write(json_path, 'dataresult.json')
                    
                    return send_file(zip_path, as_attachment=True, download_name='dataresult.zip')
                
                else:  # xlsx
                    csv_path = os.path.join(temp_dir, 'dataresult.csv')
                    with open(csv_path, 'w', newline='') as f:
                        writer = csv.DictWriter(f, fieldnames=data[0].keys())
                        writer.writeheader()
                        writer.writerows(data)
                    return send_file(csv_path, as_attachment=True, download_name='dataresult.csv')
    
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