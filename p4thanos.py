from flask import Flask, request, jsonify, render_template_string
import requests
import pandas as pd
import datetime
import numpy as np

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

# Flask application
app = Flask(__name__)

# Get data from API
def get_data(url, selected_cols):
    data = requests.get(url).json()['data']['result']
    df = pd.json_normalize(data)

    # list of values or single value in data response
    if 'values' in df.columns:
        df = df.explode('values')
        df['date'] = df['values'].apply(lambda x: datetime.datetime.utcfromtimestamp(x[0]).date())
        df['time'] = df['values'].apply(lambda x: datetime.datetime.utcfromtimestamp(x[0]).time())
        df['value'] = df['values'].apply(lambda x: x[1])
        df = df.drop(columns="values")
    elif 'value' in df.columns:
        df['date'] = df['value'].apply(lambda x: datetime.datetime.utcfromtimestamp(x[0]).date())
        df['time'] = df['value'].apply(lambda x: datetime.datetime.utcfromtimestamp(x[0]).time())
        df['value'] = df['value'].apply(lambda x: x[1])
    
    df = df.rename(columns={
        "metric.__name__": "metric_name", 
        "metric.exported_job": "station",
    })
    
    # remove columns not used
    df = df.drop(columns=[col for col in df.columns if "metric." in col]).reset_index(drop=True)

    # remove rows with no station provided
    df = df[df['station'].notnull()]
    
    # convert df to wide table
    df_result = _wide_table(df, selected_cols)
    
    # set format and replace zero values in lat-lon columns
    for col in selected_cols:
        df_result[col] = df_result[col].astype(float)
    df_result['Latitude'].replace(0, np.nan, inplace=True)
    df_result['Longitude'].replace(0, np.nan, inplace=True)

    return df_result

# function to get wide table
def _wide_table(df, selected_cols):
    df_result = pd.pivot(
        df, 
        index=['station', 'date', 'time'], 
        columns='metric_name', 
        values='value'
    ).reset_index()

    df_result = df_result[
        ['station', 'date', 'time'] + selected_cols
    ].reset_index(drop=True)

    df_result.columns.name = ""

    return df_result

# constructor of the step value for time range queries
def _get_step(number, choice):
    # convert word to code
    options = {
        "seconds": "s",
        "minutes": "m",
        "hours": "h",
        "days": "d",
        "weeks": "w",
        "years": "y",
    }
    
    # construct expression for step
    step = f"{number}{options[choice]}"

    return step

@app.route('/')
def index():
    return render_template_string('''
        <form action="/data" method="post">
            <label for="variables">Select variables:</label><br>
            <select id="variables" name="variables" multiple>
                {% for col in selected_cols %}
                    <option value="{{ col }}">{{ col }}</option>
                {% endfor %}
            </select><br><br>
            <input type="radio" id="last_register" name="option" value="last_register">
            <label for="last_register">Last register</label><br>
            <input type="radio" id="time_range" name="option" value="time_range">
            <label for="time_range">Time range</label><br><br>
            <label for="start_date">Start date:</label>
            <input type="date" id="start_date" name="start_date"><br><br>
            <label for="start_time">Start time:</label>
            <input type="time" id="start_time" name="start_time"><br><br>
            <label for="end_date">End date:</label>
            <input type="date" id="end_date" name="end_date"><br><br>
            <label for="end_time">End time:</label>
            <input type="time" id="end_time" name="end_time"><br><br>
            <label for="step_number">Step number:</label>
            <input type="number" id="step_number" name="step_number"><br><br>
            <label for="step_option">Step option:</label>
            <select id="step_option" name="step_option">
                <option value="seconds">Seconds</option>
                <option value="minutes">Minutes</option>
                <option value="hours">Hours</option>
                <option value="days">Days</option>
                <option value="weeks">Weeks</option>
                <option value="years">Years</option>
            </select><br><br>
            <input type="submit" value="Submit">
        </form>
    ''', selected_cols=selected_cols)

@app.route('/data', methods=['POST'])
def data():
    variables = request.form.getlist('variables')
    option = request.form['option']
    base_url = "http://194.242.56.226:30001/api/v1"
    query = '{job%3D"pushgateway"}'

    if option == 'last_register':
        url = f"{base_url}/query?query={query}"
    else:
        start_date = request.form['start_date']
        start_time = request.form['start_time']
        end_date = request.form['end_date']
        end_time = request.form['end_time']
        step_number = request.form['step_number']
        step_option = request.form['step_option']

        start_datetime = f"{start_date}T{start_time}:00Z"
        end_datetime = f"{end_date}T{end_time}:00Z"
        step = _get_step(step_number, step_option)

        url = f"{base_url}/query_range?query={query}&start={start_datetime}&end={end_datetime}&step={step}"

    try:
        obs = get_data(url, variables)
        return obs.to_json(orient='records')
    except Exception as e:
        return jsonify({'error': str(e)})

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)
