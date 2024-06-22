from flask import Flask, jsonify, request, render_template_string
import requests
import pandas as pd
import datetime
import numpy as np

app = Flask(__name__)

# constant
selected_cols = [
    "PM25",
    "PM25raw",
    "PM251",
    "PM252",
    "PM1",
    "CO2",
    "VOC",
    "NOx",
    "Humidity",
    "Temperature",
    "Noise",
    "NoisePeak",
    "RSSI",
    "Latitude",
    "Longitude",
    "InOut",
]

# Get data from Thanos
def get_data(url, selected_cols):
    response = requests.get(url)
    response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)
    data = response.json()['data']['result']
    df = pd.json_normalize(data)

    if 'values' in df.columns:
        df = df.explode('values')
        df['timestamp'] = df['values'].apply(lambda x: datetime.datetime.utcfromtimestamp(x[0]))
        df['value'] = df['values'].apply(lambda x: x[1])
        df = df.drop(columns="values")
    elif 'value' in df.columns:
        df['timestamp'] = df['value'].apply(lambda x: datetime.datetime.utcfromtimestamp(x[0]))
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

# function to get wide table
def _wide_table(df, selected_cols):
    df_result = pd.pivot(
        df,
        index=['station', 'timestamp'],
        columns='metric_name',
        values='value'
    ).reset_index()

    df_result = df_result[['station', 'timestamp'] + [col for col in selected_cols if col in df_result.columns]].reset_index(drop=True)
    df_result.columns.name = ""

    return df_result

# constructor of the step value for time range queries
def _get_step(number, choice):
    options = {
        "minutes": "m",
        "hours": "h",
        "days": "d",
        "weeks": "w",
        "years": "y",
    }
    step = f"{number}{options[choice]}"
    return step

@app.route('/')
def index():
    html = '''
    <!DOCTYPE html>
    <html>
    <head>
        <title>Aire Ciudadano</title>
    </head>
    <body>
        <h1>Aire Ciudadano Data Retrieval</h1>
        <form action="/api/data" method="get">
            <label for="option">Select Option:</label><br>
            <input type="radio" id="last_register" name="option" value="last_register" checked>
            <label for="last_register">Last register</label><br>
            <input type="radio" id="time_range" name="option" value="time_range">
            <label for="time_range">Time range</label><br><br>

            <label for="stats_option">Select Step Option:</label><br>
            <input type="radio" id="step_number" name="stats_option" value="step_number" checked>
            <label for="step_number">Step Number</label><br>
            <input type="radio" id="step_stats" name="stats_option" value="step_stats">
            <label for="step_stats">Step Statistics</label><br><br>

            <label for="selected_cols">Select Variables to Analyze:</label><br>
            {% for col in selected_cols %}
            <input type="checkbox" id="{{ col }}" name="selected_cols" value="{{ col }}" checked>
            <label for="{{ col }}">{{ col }}</label><br>
            {% endfor %}<br>

            <label for="starts_day">Start Date:</label><br>
            <input type="date" id="starts_day" name="starts_day" value="{{ today }}"><br><br>
            <label for="starts_hour">Start Hour:</label><br>
            <input type="time" id="starts_hour" name="starts_hour" value="00:00"><br><br>

            <label for="ends_day">End Date:</label><br>
            <input type="date" id="ends_day" name="ends_day" value="{{ today }}"><br><br>
            <label for="ends_hour">End Hour:</label><br>
            <input type="time" id="ends_hour" name="ends_hour" value="00:00"><br><br>

            <label for="step_number">Step Number:</label><br>
            <input type="number" id="step_number" name="step_number" value="1"><br><br>
            <label for="step_option">Step Option:</label><br>
            <select id="step_option" name="step_option">
                <option value="minutes">Minutes</option>
                <option value="hours" selected>Hours</option>
                <option value="days">Days</option>
                <option value="weeks">Weeks</option>
                <option value="years">Years</option>
            </select><br><br>

            <input type="submit" value="Retrieve Data">
        </form>
    </body>
    </html>
    '''
    return render_template_string(html, selected_cols=selected_cols, today=str(datetime.date.today()))

@app.route('/api/data', methods=['GET'])
def get_thanos_data():
    option = request.args.get('option', default='last_register', type=str)
    stats_option = request.args.get('stats_option', default='step_number', type=str)
    starts_day = request.args.get('starts_day', default=str(datetime.date.today()), type=str)
    starts_hour = request.args.get('starts_hour', default='00:00', type=str)
    ends_day = request.args.get('ends_day', default=str(datetime.date.today()), type=str)
    ends_hour = request.args.get('ends_hour', default='00:00', type=str)
    step_number = request.args.get('step_number', default=1, type=int)
    step_option = request.args.get('step_option', default='hours', type=str)
    selected_vars = request.args.getlist('selected_cols')

    try:
        # query to get all data
        query = '{job="pushgateway"}'

        # last registers selected
        if option == 'last_register':
            url = f"http://194.242.56.226:30001/api/v1/query?query={query}"
        # range of time selected
        elif option == 'time_range':
            start_datetime = f"{starts_day}T{starts_hour}:00Z"
            end_datetime = f"{ends_day}T{ends_hour}:00Z"
            step = _get_step(step_number, step_option)
            url = f"http://194.242.56.226:30001/api/v1/query_range?query={query}&start={start_datetime}&end={end_datetime}&step=1m"

        obs = get_data(url, selected_vars)

        if stats_option == 'step_stats':
            obs.set_index(['timestamp'], inplace=True)
            resampled_list = []

            for station, group in obs.groupby('station'):
                resampled = group.resample(step).agg({
                    col: ['mean', 'max', 'min', 'last'] for col in selected_vars
                }).reset_index()
                resampled['station'] = station
                resampled_list.append(resampled)

            obs = pd.concat(resampled_list, axis=0)
            obs.columns = ['_'.join(col).strip() if col[1] else col[0] for col in obs.columns.values]

        data_json = obs.to_json(orient='records')
        return jsonify({'status': 'success', 'data': data_json}), 200

    except ValueError:
        return jsonify({'status': 'error', 'message': 'Nothing found.'}), 404

    except Exception as error:
        return jsonify({'status': 'error', 'message': str(error)}), 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5001)
