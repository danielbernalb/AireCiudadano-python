from flask import Flask, request, jsonify, render_template_string
import pandas as pd
import datetime
import numpy as np
import json
import aiohttp
import asyncio
from async_lru import alru_cache

# Constants
selected_cols = [
    "PM25", "PM25raw", "PM251", "PM252", "PM1", "CO2", "VOC", "NOx",
    "Humidity", "Temperature", "Noise", "NoisePeak", "RSSI", "Latitude",
    "Longitude", "InOut",
]

# Flask application
app = Flask(__name__)

# Function to fetch Prometheus data asynchronously
async def fetch_prometheus_data(session, query, start, end, step='1m'):
    url = 'http://194.242.56.226:30000/api/v1/query_range'  # Replace with your Prometheus URL
    params = {
        'query': query,
        'start': start,
        'end': end,
        'step': step
    }
    try:
        async with session.get(url, params=params) as response:
            response.raise_for_status()
            data = await response.json()
            return data
    except aiohttp.ClientError as e:
        app.logger.error(f"Error fetching data from Prometheus: {e}")
        return None

# Function to get data from API asynchronously with caching
@alru_cache(maxsize=32)
async def get_data(query, start_datetime, end_datetime, step):
    async with aiohttp.ClientSession() as session:
        tasks = []
        current_time = start_datetime
        while current_time < end_datetime:
            next_time = current_time + datetime.timedelta(hours=1)
            tasks.append(fetch_prometheus_data(session, query, current_time.timestamp(), next_time.timestamp(), step))
            current_time = next_time

        results = await asyncio.gather(*tasks)
        all_data = []
        for result in results:
            if result and 'data' in result:
                all_data.extend(result['data']['result'])

        if not all_data:
            return pd.DataFrame()

        df = pd.json_normalize(all_data)
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

# Function to get wide table
def _wide_table(df, selected_cols):
    df_grouped = df.groupby(['station', 'date', 'metric_name']).first()['value'].unstack().reset_index()
    df_result = pd.DataFrame(columns=['station', 'date'] + selected_cols)
    df_result[['station', 'date']] = df_grouped[['station', 'date']]
    for col in selected_cols:
        if col in df_grouped.columns:
            df_result[col] = df_grouped[col]
        else:
            df_result[col] = np.nan
    df_result.columns.name = ""
    return df_result

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
    aggregation_method = request.args.get('aggregation_method', 'step')

    return render_template_string('''
        <form action="/dataresult" method="post">
            <label for="variables">Select variables:</label><br>
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
            <input type="time" id="start_time" name="start_time" value="{{ start_time }}"><br><br>
            <label for="end_date">End date/time:</label>
            <input type="date" id="end_date" name="end_date" value="{{ end_date }}">
            <label for="end_time"> / </label>
            <input type="time" id="end_time" name="end_time" value="{{ end_time }}"><br><br>
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
async def data():
    variables = request.form.getlist('variables')
    start_date = request.form.get('start_date', '2024-05-09')
    start_time = request.form.get('start_time', '08:00')
    end_date = request.form.get('end_date', '2024-05-09')
    end_time = request.form.get('end_time', '10:00')
    step_number = request.form.get('step_number', '1')
    step_option = request.form.get('step_option', 'hours')
    aggregation_method = request.form.get('aggregation_method', 'step')

    start_datetime = datetime.datetime.strptime(f"{start_date} {start_time}", "%Y-%m-%d %H:%M")
    end_datetime = datetime.datetime.strptime(f"{end_date} {end_time}", "%Y-%m-%d %H:%M")
    step = _get_step(step_number, step_option)

    delta = end_datetime - start_datetime
    days = delta.total_seconds() / 86400
    if days > 7:
        return jsonify({'error': 'The date range cannot exceed 7 days'})

    query = 'up'  # Replace with your Prometheus query
    try:
        obs = await get_data(query, start_datetime, end_datetime, step)

        total_records = obs.shape[0]
        total_pages = (total_records + page_size - 1) // page_size
        obs = obs.iloc[(page - 1) * page_size: page * page_size]

        # Convert DataFrame to dictionary and replace NaN with None explicitly
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
            'total_pages': total_pages,
            'page': page,
            'page_size': page_size,
            'data': grouped_data
        })
    except Exception as e:
        app.logger.error(f'Error in data endpoint: {str(e)}')
        return jsonify({'error': str(e)})

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
