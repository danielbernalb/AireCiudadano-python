# pokzip: probando zip, seguir probando

from flask import Flask, request, jsonify, render_template_string, send_file
import requests
import pandas as pd
import datetime
import numpy as np
import json
import os
import zipfile
import time

# Constants
selected_cols = [
    "PM25", "PM25raw", "PM1", "Humidity", "Temperature",
]

# Flask application
app = Flask(__name__)

# Get data from API with time intervals
def get_data(url, selected_cols, start_datetime, end_datetime, step, interval_seconds):
    all_results = []
    current_start_time = start_datetime
    while current_start_time < end_datetime:
        current_end_time = min(current_start_time + datetime.timedelta(seconds=interval_seconds), end_datetime)

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

    final_df = pd.concat(all_results, ignore_index=True).drop_duplicates(subset=['date', 'station'])
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
            <h2>API AIRECIUDADANO v1.0</h2>
            <h3>Instructions at: <a href="https://aireciudadano.com/apidata/" target="_blank">aireciudadano.com/apidata</a></h3><br>
            <label for="variables">Select variables pok 5:</label><br>
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

            <!-- New option for result format -->
            <label for="result_format">Result format:</label>
            <select id="result_format" name="result_format">
                <option value="screen">Result in screen</option>
                <option value="file">Result in JSON file</option>
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

    if aggregation_method == 'average':
        step = '1m'
    else:
        step = _get_step(step_number, step_option)

    url = f"{base_url}/query_range?query={query}"

    try:
        if aggregation_method == 'average':
            interval_seconds=3539
            obs = get_data(url, variables, start_datetime_adjusted, end_datetime, step, interval_seconds)
        else:
            interval_seconds=3600
            obs = get_data(url, variables, start_datetime, end_datetime, step, interval_seconds)

        if station_filter:
            filters = station_filter.split(',')
            obs = obs[obs['station'].str.contains('|'.join(filters), case=False)]

        if aggregation_method == 'step':
            # Convertir la columna 'date' a tipo datetime
            obs['date'] = pd.to_datetime(obs['date'], utc=True)

            # Filtrar para incluir la hora de inicio exacta y evitar duplicados en los intervalos
            mask_start = obs['date'] == pd.to_datetime(start_datetime, utc=True)
            mask_step = (obs['date'] > pd.to_datetime(start_datetime, utc=True)) & (obs['date'] <= pd.to_datetime(end_datetime, utc=True))
            obs = obs[mask_start | mask_step]

        elif aggregation_method == 'average':
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


        if result_format == "screen":
            return jsonify({
                'total_records': total_records,
                'data': grouped_data
            })
        else:
            # Save JSON data to file
            json_filename = 'dataresult.json'
            with open(json_filename, 'w') as json_file:
                json.dump(grouped_data, json_file, indent=4)

            # Compress JSON file to ZIP
            zip_filename = 'dataresult.zip'
            with zipfile.ZipFile(zip_filename, 'w') as zipf:
                zipf.write(json_filename, compress_type=zipfile.ZIP_DEFLATED)

            # Remove the JSON file after compressing
            os.remove(json_filename)

            return send_file(zip_filename, as_attachment=True)

    except Exception as e:
        app.logger.error(f'Error in data endpoint: {str(e)}')
        return jsonify({'error': str(e)})

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)