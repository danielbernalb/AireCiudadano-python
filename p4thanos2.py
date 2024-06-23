from flask import Flask, request, jsonify, render_template_string
import requests
import pandas as pd
import datetime
import numpy as np

# Constant
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
    try:
        data = requests.get(url).json()['data']['result']
        df = pd.json_normalize(data)

        # list of values or single value in data response
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
        
        # remove columns not used
        df = df.drop(columns=[col for col in df.columns if "metric." in col]).reset_index(drop=True)

        # remove rows with no station provided
        df = df[df['station'].notnull()]
        
        # convert df to wide table
        df_result = _wide_table(df, selected_cols)
        
        # set format and replace zero values in lat-lon columns if they are in the selected columns
        for col in selected_cols:
            if col in df_result.columns:
                df_result[col] = df_result[col].astype(float)
        if 'Latitude' in df_result.columns:
            df_result['Latitude'].replace(0, np.nan, inplace=True)
        if 'Longitude' in df_result.columns:
            df_result['Longitude'].replace(0, np.nan, inplace=True)

        return df_result
    except Exception as e:
        app.logger.error(f'Error in get_data: {str(e)}')
        raise

# function to get wide table
def _wide_table(df, selected_cols):
    try:
        df_result = pd.pivot(
            df, 
            index=['station', 'date'], 
            columns='metric_name', 
            values='value'
        ).reset_index()
        
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

# constructor of the step value for time range queries
def _get_step(number, choice):
    # convert word to code
    options = {
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
    variables = request.args.getlist('variables') or selected_cols
    start_datetime = request.args.get('start_datetime', '2024-05-09T08:00')
    end_datetime = request.args.get('end_datetime', '2024-05-09T10:00')
    step_number = request.args.get('step_number', '1')
    step_option = request.args.get('step_option', 'hours')

    return render_template_string('''
        <form action="/data" method="post">
            <label for="variables">Select variables:</label><br>
            <input type="checkbox" id="select_all" onclick="toggle(this);">
            <label for="select_all">Select/Deselect All</label><br>
            {% for col in selected_cols %}
                <input type="checkbox" id="{{ col }}" name="variables" value="{{ col }}" {% if col in variables %}checked{% endif %}>
                <label for="{{ col }}">{{ col }}</label><br>
            {% endfor %}
            <br>
            <label for="start_datetime">Start date and time:</label>
            <input type="datetime-local" id="start_datetime" name="start_datetime" value="{{ start_datetime }}"><br><br>
            <label for="end_datetime">End date and time:</label>
            <input type="datetime-local" id="end_datetime" name="end_datetime" value="{{ end_datetime }}"><br><br>
            <label for="step_number">Step number:</label>
            <input type="number" id="step_number" name="step_number" value="{{ step_number }}"><br><br>
            <label for="step_option">Step option:</label>
            <select id="step_option" name="step_option">
                <option value="minutes" {% if step_option == 'minutes' %}selected{% endif %}>Minutes</option>
                <option value="hours" {% if step_option == 'hours' %}selected{% endif %}>Hours</option>
                <option value="days" {% if step_option == 'days' %}selected{% endif %}>Days</option>
                <option value="weeks" {% if step_option == 'weeks' %}selected{% endif %}>Weeks</option>
                <option value="years" {% if step_option == 'years' %}selected{% endif %}>Years</option>
            </select><br><br>
            <input type="submit" value="Submit">
        </form>
        <script>
            function toggle(source) {
                checkboxes = document.getElementsByName('variables');
                for (var i = 0, n = checkboxes.length; i < n; i++) {
                    checkboxes[i].checked = source.checked;
                }
            }
        </script>
    ''', selected_cols=selected_cols, variables=variables, 
       start_datetime=start_datetime, end_datetime=end_datetime, 
       step_number=step_number, step_option=step_option)

@app.route('/data', methods=['POST'])
def data():
    variables = request.form.getlist('variables')
    base_url = "http://194.242.56.226:30001/api/v1"
    query = '{job%3D"pushgateway"}'

    start_datetime = request.form['start_datetime']
    end_datetime = request.form['end_datetime']
    step_number = request.form['step_number']
    step_option = request.form['step_option']

    step = _get_step(step_number, step_option)

    url = f"{base_url}/query_range?query={query}&start={start_datetime}Z&end={end_datetime}Z&step={step}"

    try:
        obs = get_data(url, variables)
        json_data = obs.drop(columns='time').to_dict(orient='records')
        
        # Agrupar por 'station'
        grouped_data = {}
        for record in json_data:
            station = record.pop('station')
            if station not in grouped_data:
                grouped_data[station] = []
            grouped_data[station].append(record)
        
        return jsonify(grouped_data)
    except Exception as e:
        app.logger.error(f'Error in data endpoint: {str(e)}')
        return jsonify({'error': str(e)})

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)
