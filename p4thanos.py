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

    # Ensure all selected columns are present in the dataframe
    for col in selected_cols:
        if col not in df_result.columns:
            df_result[col] = np.nan

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

            <label for="stats_option">Select Statistics Option:</label><br>
            <input type="radio" id="step_number" name="stats_option" value="step_number" checked>
            <label for="step_number">Step Number</label><br>
            <input type="radio" id="step_stats" name="stats_option" value="step_stats">
            <label for="step_stats">Step Statistics</label><br><br>

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
                <option value="seconds">Seconds</option>
                <option value="minutes">Minutes</option>
                <option value="hours" selected>Hours</option>
                <option value="days">Days</option>
                <option value="weeks">Weeks</option>
                <option value="years">Years</option>
            </select><br><br>

            <label for="variables">Select Variables to Analyze:</label><br>
            <select id="variables" name="variables" multiple>
                {% for col in selected_cols %}
                <option value="{{ col }}">{{ col }}</option>
                {% endfor %}
            </select><br><br>

            <input type="submit" value="Retrieve Data">
        </form>
    </body>
    </html>
    '''
    return render_template_string(html, today=str(datetime.date.today()), selected_cols=selected_cols)

@app.route('/api/data', methods=['GET'])
def get_thanos_data():
    option = request.args.get('option', default='last_register', type=str)
    stats_option = request.args.get('stats_option', default='step_number', type=str)
    starts_day = request.args.get('starts_day', default=str(datetime.date.today()), type=str)
    starts_hour = request.args.get('starts_hour', default='00:00', type=str)
    ends_day = request.args.get('ends_day', default=str(datetime.date.today()), type=str)
    ends_hour = request.args.get('ends_hour', default='00:00', type:str)
    step_number = request.args.get('step_number', default=1, type=int)
    step_option = request.args.get('step_option', default='hours', type=str)
    variables = request.args.getlist('variables')

    # Si no se seleccionan variables, se usan todas las columnas por defecto
    if not variables:
        variables = selected_cols

    try:
        # query to get all data
        query = '{job="pushgateway"}'

        # last registers selected
        if option == 'last_register':
            url = f"http://194.242.56.226:30001/api/v1/query?query={query}"
        # range of time selected
        elif option == 'time_range':
            # construct start_datetime
            start_datetime = f"{starts_day}T{starts_hour}:00Z"
            # construct end_datetime
            end_datetime = f"{ends_day}T{ends_hour}:00Z"
            # construct step                
            step = _get_step(step_number, step_option)
            url = f"http://194.242.56.226:30001/api/v1/query_range?query={query}&start={start_datetime}&end={end_datetime}&step={step}"
        
        # get obs from API, using the url created before
        obs = get_data(url, variables)
        
        # Perform step statistics if selected
        if stats_option == 'step_stats':
            obs_stats = obs.describe().to_dict()

        # convert dataframe to json
        data_json = obs.to_json(orient='records')
        
        if stats_option == 'step_stats':
            return jsonify({'status': 'success', 'data': data_json, 'stats': obs_stats}), 200
        else:
            return jsonify({'status': 'success', 'data': data_json}), 200

    except ValueError:
        return jsonify({'status': 'error', 'message': 'Nothing found.'}), 404

    except Exception as error:
        return jsonify({'status': 'error', 'message': str(error)}), 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5001)
