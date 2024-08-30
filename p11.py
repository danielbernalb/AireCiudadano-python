from flask import Flask, request, jsonify, render_template_string
import requests
import pandas as pd
import datetime
import numpy as np
import time
from urllib.parse import quote

# Constants
selected_cols = [
    "PM25", "PM25raw", "PM251", "PM252", "PM1", "CO2", "VOC", "NOx",
    "Humidity", "Temperature", "Noise", "NoisePeak", "RSSI", "Latitude",
    "Longitude", "InOut",
]

# Flask application
app = Flask(__name__)

# Get data from API with time intervals
def get_data(url, selected_cols, start_datetime, end_datetime, step, interval_minutes=60):
    all_results = []
    current_start_time = start_datetime

    while current_start_time < end_datetime:
        current_end_time = min(current_start_time + datetime.timedelta(minutes=interval_minutes), end_datetime)
        query_url = f"{url}&start={current_start_time.isoformat()}Z&end={current_end_time.isoformat()}Z&step={step}"
        
        try:
            response = requests.get(query_url)
            response.raise_for_status()
            data = response.json()['data']['result']
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

            all_results.append(df_result)

        except Exception as e:
            app.logger.error(f'Error fetching data chunk: {str(e)}')
            raise

        current_start_time = current_end_time

        # Pausa para evitar sobrecarga del servidor
        time.sleep(1)

    final_df = pd.concat(all_results, ignore_index=True)
    return final_df

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
            <label for="variables">Select variables 2:</label><br>
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
    variables = request.form.getlist('variables')
    base_url = "http://194.242.56.226:30000/api/v1"
    query = '{job="pushgateway"}'

    start_date = request.form['start_date']
    start_time = request.form['start_time']
    end_date = request.form['end_date']
    end_time = request.form['end_time']
    step_number = request.form['step_number']
    step_option = request.form['step_option']
    aggregation_method = request.form['aggregation_method']
    station_filter = request.form.get('station_filter', '')

    start_datetime = pd.to_datetime(f"{start_date}T{start_time}").tz_localize('UTC')
    end_datetime = pd.to_datetime(f"{end_date}T{end_time}").tz_localize('UTC')

    if aggregation_method == 'average':
        query_start = start_datetime - pd.Timedelta(hours=1, minutes=59, seconds=59)
        step = '1m'
    else:
        query_start = start_datetime
        step = _get_step(step_number, step_option)

    try:
        # Construir la URL de consulta correctamente
        encoded_query = quote(query)
        query_url = f"{base_url}/query_range?query={encoded_query}&start={query_start.isoformat()}&end={end_datetime.isoformat()}&step={step}"
        
        # Realizar la solicitud a Prometheus
        response = requests.get(query_url)
        response.raise_for_status()  # Esto levantará una excepción para códigos de estado HTTP no exitosos
        data = response.json()['data']['result']
        
        # Procesar los datos...
        obs = pd.json_normalize(data)
        
        if 'values' in obs.columns:
            obs = obs.explode('values')
            obs['date'] = obs['values'].apply(lambda x: pd.to_datetime(x[0], unit='s', utc=True))
            obs['value'] = obs['values'].apply(lambda x: x[1])
            obs = obs.drop(columns="values")
        elif 'value' in obs.columns:
            obs['date'] = obs['value'].apply(lambda x: pd.to_datetime(x[0], unit='s', utc=True))
            obs['value'] = obs['value'].apply(lambda x: x[1])

        obs = obs.rename(columns={
            "metric.__name__": "metric_name",
            "metric.exported_job": "station",
        })

        obs = obs.drop(columns=[col for col in obs.columns if "metric." in col]).reset_index(drop=True)
        obs = obs[obs['station'].notnull()]

        if station_filter:
            filters = station_filter.split(',')
            obs = obs[obs['station'].str.contains('|'.join(filters), case=False)]

        if aggregation_method == 'average':
            obs['date'] = pd.to_datetime(obs['date'], utc=True)
            obs.set_index(['station', 'date'], inplace=True)

            obs = obs.apply(pd.to_numeric, errors='coerce')
            hourly_obs = []

            for station, group in obs.groupby('station'):
                current_time = start_datetime
                while current_time <= end_datetime:
                    mask = (group.index.get_level_values('date') > (current_time - pd.Timedelta(hours=1))) & (group.index.get_level_values('date') <= current_time)
                    hourly_avg = group.loc[mask].mean()
                    hourly_avg['station'] = station
                    hourly_avg['date'] = current_time.strftime('%Y-%m-%dT%H:%M:%SZ')
                    hourly_obs.append(hourly_avg)
                    current_time += pd.Timedelta(hours=1)

            obs = pd.DataFrame(hourly_obs).reset_index(drop=True)

        total_records = obs.shape[0]
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

        return jsonify({
            'total_records': total_records,
            'data': grouped_data
        })
    except requests.RequestException as e:
        app.logger.error(f'Error in Prometheus query: {str(e)}')
        return jsonify({'error': f'Error in Prometheus query: {str(e)}'}), 400
    except Exception as e:
        app.logger.error(f'Error in data endpoint: {str(e)}')
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)