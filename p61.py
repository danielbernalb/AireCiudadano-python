from flask import Flask, request, jsonify, render_template_string
import requests
import pandas as pd
import datetime
import numpy as np
import json

# Constants
selected_cols = [
    "PM25", "PM25raw", "PM251", "PM252", "PM1", "CO2", "VOC", "NOx",
    "Humidity", "Temperature", "Noise", "NoisePeak", "RSSI", "Latitude",
    "Longitude", "InOut",
]

# Flask application
app = Flask(__name__)

# Get data from API with pagination support
def get_data(url, selected_cols, start, limit):
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

        # Pagination: return only the subset of data based on start and limit
        return df_result.iloc[start:start + limit]
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

    try:
        # Implementing pagination
        page = int(request.args.get('page', 1))
        limit = int(request.args.get('limit', 1000))
        start = (page - 1) * limit

        obs = get_data(url, variables, start, limit)
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

        # Filter the results to ensure dates are within the original specified range
        obs = obs[(obs['date'] >= start_datetime) & (obs['date'] <= end_datetime)]

        total_records = obs.shape[0]

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
            'data': grouped_data
        })
    except Exception as e:
        app.logger.error(f'Error in data endpoint: {str(e)}')
        return jsonify({'error': str(e)})

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)
