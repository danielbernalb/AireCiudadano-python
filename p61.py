from flask import Flask, request, jsonify, render_template_string
import requests
import pandas as pd
import datetime
import numpy as np
import json
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
import logging

# Configurar logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Constants
selected_cols = [
    "PM25", "PM25raw", "PM251", "PM252", "PM1", "CO2", "VOC", "NOx",
    "Humidity", "Temperature", "Noise", "NoisePeak", "RSSI", "Latitude",
    "Longitude", "InOut",
]

# Flask application
app = Flask(__name__)

# Get data from API
def get_data(url, selected_cols):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Esto levantará una excepción para códigos de estado HTTP no exitosos
        json_response = response.json()
        
        logger.debug(f"API Response: {json_response}")  # Log de la respuesta completa
        
        if 'data' not in json_response:
            raise KeyError(f"'data' key not found in API response. Response keys: {list(json_response.keys())}")
        
        if 'result' not in json_response['data']:
            raise KeyError(f"'result' key not found in 'data'. 'data' keys: {list(json_response['data'].keys())}")
        
        data = json_response['data']['result']
        df = pd.json_normalize(data)

        if 'values' in df.columns:
            df = df.explode('values')
            df['date'] = df['values'].apply(lambda x: datetime.datetime.utcfromtimestamp(x[0]).isoformat())
            df['value'] = df['values'].apply(lambda x: x[1])
            df = df.drop(columns="values")
        elif 'value' in df.columns:
            df['date'] = df['value'].apply(lambda x: datetime.datetime.utcfromtimestamp(x[0]).isoformat())
            df['value'] = df['value'].apply(lambda x: x[1])

        df = df.rename(columns={
            "metric.__name__": "metric_name",
            "metric.exported_job": "station",
        })

        df = df.drop(columns=[col for col in df.columns if "metric." in col]).reset_index(drop=True)
        df = df[df['station'].notnull()]

        df_result = _wide_table(df, selected_cols)

        for col in selected_cols:
            if col in df_result.columns:
                df_result[col] = df_result[col].astype(float)
        if 'Latitude' in df_result.columns:
            df_result['Latitude'].replace(0, np.nan, inplace=True)
        if 'Longitude' in df_result.columns:
            df_result['Longitude'].replace(0, np.nan, inplace=True)

        return df_result
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        raise
    except KeyError as e:
        logger.error(f"KeyError in get_data: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in get_data: {e}")
        raise

# Function to get wide table
def _wide_table(df, selected_cols):
    try:
        df_result = pd.pivot(df, index=['station', 'date'], columns='metric_name', values='value').reset_index()
        all_cols = ['station', 'date'] + selected_cols
        missing_cols = set(all_cols) - set(df_result.columns)
        for col in missing_cols:
            df_result[col] = np.nan
        df_result = df_result[all_cols].reset_index(drop=True)
        df_result.columns.name = ""
        return df_result
    except Exception as e:
        logger.error(f'Pivot Error: {str(e)}')
        raise

# Constructor of the step value for time range queries
def _get_step(number, choice):
    options = {"minutes": "m", "hours": "h", "days": "d", "weeks": "w", "years": "y"}
    return f"{number}{options[choice]}"

def process_batch(start_datetime, end_datetime, variables, step, base_url, query):
    url = f"{base_url}/query_range?query={query}&start={start_datetime}&end={end_datetime}&step={step}"
    return get_data(url, variables)

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
            <label for="variables">Select variables 78:</label><br>
            <input type="checkbox" id="select_all" onclick="toggle(this);">
            <label for="select_all">Select/Deselect All</label><br>
            {% for col in selected_cols %}
                <input type="checkbox" id="{{ col }}" name="variables" value="{{ col }}" {% if col in variables %}checked{% endif %}>
                <label for="{{ col }}">{{ col }}</label><br>
            {% endfor %}
            <br>
            <label for="start_date">Start date/time:</label>
            <input type="date" id="start_date" name="start_date" value="{{ start_date }}">
            <label for="start_time"> / </label>
            <input type="time" id="start_time" name="start_time" value="{{ start_time }}" step="3600" list="hour-markers"><br><br>
            <label for="end_date">End date/time:</label>
            <input type="date" id="end_date" name="end_date" value="{{ end_date }}">
            <label for="end_time"> / </label>
            <input type="time" id="end_time" name="end_time" value="{{ end_time }}" step="3600" list="hour-markers"><br><br>
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
        base_url = "http://88.99.187.134:30000/api/v1"
        query = '{job%3D"pushgateway"}'

        start_date = request.form['start_date']
        start_time = request.form['start_time']
        end_date = request.form['end_date']
        end_time = request.form['end_time']
        step_number = request.form['step_number']
        step_option = request.form['step_option']
        aggregation_method = request.form['aggregation_method']
        station_filter = request.form.get('station_filter', '')

        start_datetime = parse(f"{start_date}T{start_time}:00Z")
        end_datetime = parse(f"{end_date}T{end_time}:00Z")

        if aggregation_method == 'average':
            step = '1m'
        else:
            step = _get_step(step_number, step_option)

        # Definir el tamaño del lote (por ejemplo, 6 horas)
        batch_size = relativedelta(hours=6)

        all_data = []
        current_start = start_datetime
        while current_start < end_datetime:
            current_end = min(current_start + batch_size, end_datetime)
            try:
                batch_data = process_batch(current_start.isoformat(), current_end.isoformat(), variables, step, base_url, query)
                all_data.append(batch_data)
            except Exception as e:
                logger.error(f"Error processing batch from {current_start} to {current_end}: {e}")
                # Opcionalmente, podrías decidir continuar con el siguiente lote en lugar de detener todo el proceso
                # Si prefieres detener todo el proceso, descomenta la siguiente línea:
                # raise
            current_start = current_end

        if not all_data:
            return jsonify({'error': 'No se pudo obtener ningún dato válido'}), 500

        obs = pd.concat(all_data, ignore_index=True)

        if station_filter:
            filters = station_filter.split(',')
            obs = obs[obs['station'].str.contains('|'.join(filters), case=False)]

        if aggregation_method == 'average':
            obs['date'] = pd.to_datetime(obs['date'], utc=True)
            obs.set_index(['station', 'date'], inplace=True)

            obs = obs.apply(pd.to_numeric, errors='coerce')
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

            obs = pd.DataFrame(hourly_obs).reset_index(drop=True)

        # Filtrar los resultados para asegurar que las fechas estén dentro del rango original especificado
        obs = obs[(obs['date'] >= start_datetime.isoformat()) & (obs['date'] <= end_datetime.isoformat())]

        total_records = obs.shape[0]

        # Convertir DataFrame a diccionario y reemplazar NaN con None explícitamente
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

        # Implementar paginación
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 1000, type=int)
        
        paginated_data = {}
        for station, records in grouped_data.items():
            start = (page - 1) * per_page
            end = start + per_page
            paginated_data[station] = records[start:end]

        return jsonify({
            'total_records': total_records,
            'current_page': page,
            'per_page': per_page,
            'total_pages': (total_records + per_page - 1) // per_page,
            'data': paginated_data
        })

    except Exception as e:
        logger.error(f"Error in data endpoint: {str(e)}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)