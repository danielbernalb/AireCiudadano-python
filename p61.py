from flask import Flask, request, jsonify, render_template_string, url_for
import requests
import pandas as pd
import datetime
import numpy as np
import json
from celery import Celery

# Flask and Celery configuration
app = Flask(__name__)
app.config['CELERY_BROKER_URL'] = 'redis://localhost:6379/0'
app.config['CELERY_RESULT_BACKEND'] = 'redis://localhost:6379/0'

celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)

# Constants
selected_cols = [
    "PM25", "PM25raw", "PM251", "PM252", "PM1", "CO2", "VOC", "NOx",
    "Humidity", "Temperature", "Noise", "NoisePeak", "RSSI", "Latitude",
    "Longitude", "InOut",
]

@celery.task(bind=True)
def get_data_task(self, url, selected_cols):
    try:
        data = requests.get(url).json()['data']['result']
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

        return df_result.to_dict(orient='records')
    except Exception as e:
        app.logger.error(f'Error in get_data: {str(e)}')
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
    variables = request.form.getlist('variables')
    base_url = "http://194.242.56.226:30000/api/v1"
    query = '{job%3D"pushgateway"}'
    start_date = request.form['start_date']
    start_time = request.form['start_time']
    end_date = request.form['end_date']
    end_time = request.form['end_time']
    step_number = request.form['step_number']
    step_option = request.form['step_option']
    aggregation_method = request.form['aggregation_method']
    station_filter = request.form.get('station_filter', '')

    start_datetime = f"{start_date}T{start_time}:00Z"
    start_datetime_adjusted = (datetime.datetime.fromisoformat(start_datetime[:-1]) - datetime.timedelta(hours=1)).isoformat() + 'Z'
    end_datetime = f"{end_date}T{end_time}:00Z"

    if aggregation_method == 'average':
        step = '1m'
    else:
        step = _get_step(step_number, step_option)

    url = f"{base_url}/query_range?query={query}&start={start_datetime_adjusted}&end={end_datetime}&step={step}"

    # Trigger asynchronous task
    task = get_data_task.apply_async(args=[url, variables])

    return jsonify({'task_id': task.id}), 202

@app.route('/status/<task_id>')
def taskstatus(task_id):
    task = get_data_task.AsyncResult(task_id)
    if task.state == 'PENDING':
        response = {'state': task.state, 'status': 'Pending...'}
    elif task.state != 'FAILURE':
        response = {'state': task.state, 'result': task.result}
    else:
        response = {'state': task.state, 'status': str(task.info)}
    return jsonify(response)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)
