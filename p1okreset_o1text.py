# p1okclaude

from flask import Flask, request, jsonify, render_template_string
import requests
import pandas as pd
import datetime
import numpy as np
import json
import dask.dataframe as dd
from typing import List, Dict, Any
import gc

# Constants
selected_cols = [
    "PM25", "PM25raw", "PM1", "Humidity", "Temperature",
]

CHUNK_SIZE = 100000  # Adjust based on available memory
MAX_WORKERS = 4  # Adjust based on available CPU cores

app = Flask(__name__)

def optimize_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Optimize DataFrame memory usage by converting to efficient dtypes."""
    # Convert float64 to float32
    float_cols = df.select_dtypes(include=['float64']).columns
    for col in float_cols:
        df[col] = df[col].astype('float32')
    
    # Convert string columns to categorical
    str_cols = df.select_dtypes(include=['object']).columns
    for col in str_cols:
        if col != 'date':  # Don't convert date column
            df[col] = df[col].astype('category')
    
    return df

def get_data(url: str, selected_cols: List[str]) -> pd.DataFrame:
    """Get data from API with memory optimization."""
    try:
        data = requests.get(url).json()['data']['result']
        
        # Process data in chunks
        chunks = []
        chunk_size = CHUNK_SIZE
        
        for i in range(0, len(data), chunk_size):
            chunk_data = data[i:i + chunk_size]
            df_chunk = pd.json_normalize(chunk_data)
            
            if 'values' in df_chunk.columns:
                df_chunk = df_chunk.explode('values')
                df_chunk['date'] = df_chunk['values'].apply(
                    lambda x: datetime.datetime.utcfromtimestamp(x[0]).isoformat()
                )
                df_chunk['value'] = df_chunk['values'].apply(lambda x: x[1])
                df_chunk = df_chunk.drop(columns="values")
            elif 'value' in df_chunk.columns:
                df_chunk['date'] = df_chunk['value'].apply(
                    lambda x: datetime.datetime.utcfromtimestamp(x[0]).isoformat()
                )
                df_chunk['value'] = df_chunk['value'].apply(lambda x: x[1])
            
            # Rename columns before filtering
            df_chunk = df_chunk.rename(columns={
                "metric.__name__": "metric_name",
                "metric.exported_job": "station",
            })
            
            # Drop unnecessary columns
            df_chunk = df_chunk.drop(
                columns=[col for col in df_chunk.columns if "metric." in col]
            ).reset_index(drop=True)
            
            # Filter null stations and selected metrics
            df_chunk = df_chunk[
                df_chunk['station'].notnull() & 
                df_chunk['metric_name'].isin(selected_cols)
            ]
            
            chunks.append(df_chunk)
            
            # Force garbage collection
            gc.collect()
        
        df = pd.concat(chunks, ignore_index=True)
        df_result = _wide_table(df, selected_cols)
        df_result = optimize_dtypes(df_result)
        
        # Convert value columns to float
        for col in selected_cols:
            if col in df_result.columns:
                df_result[col] = pd.to_numeric(df_result[col], errors='coerce')
        
        return df_result
    except Exception as e:
        app.logger.error(f'Error in get_data: {str(e)}')
        raise

def _wide_table(df: pd.DataFrame, selected_cols: List[str]) -> pd.DataFrame:
    """Create wide table with memory optimization."""
    try:
        # Convert to pandas DataFrame if it's a Dask DataFrame
        if isinstance(df, dd.DataFrame):
            df = df.compute()
        
        # Filter data to include only selected metrics
        df = df[df['metric_name'].isin(selected_cols)]
        
        # Create pivot table using pandas
        df_result = df.pivot_table(
            index=['station', 'date'],
            columns='metric_name',
            values='value',
            aggfunc='first'  # Use 'first' to maintain original values
        ).reset_index()
        
        # Ensure all selected columns exist
        for col in selected_cols:
            if col not in df_result.columns:
                df_result[col] = np.nan
        
        # Reorder columns to match selected_cols order
        final_cols = ['station', 'date'] + selected_cols
        df_result = df_result[final_cols]
        
        # Reset column names
        df_result.columns.name = ""
        
        return df_result
    except Exception as e:
        app.logger.error(f'Pivot Error: {str(e)}')
        raise

def process_hourly_averages(obs: pd.DataFrame, start_datetime: str, end_datetime: str) -> pd.DataFrame:
    """Process hourly averages using efficient chunking."""
    try:
        # Convert date column to datetime if it's not already
        obs['date'] = pd.to_datetime(obs['date'], utc=True)
        
        # Process each station separately to manage memory
        results = []
        for station in obs['station'].unique():
            station_data = obs[obs['station'] == station].copy()
            station_data.set_index('date', inplace=True)
            
            # Get numeric columns excluding 'station'
            numeric_cols = station_data.select_dtypes(include=[np.number]).columns
            
            # Resample and calculate mean for numeric columns
            hourly_data = station_data[numeric_cols].resample('1h').mean()
            hourly_data['station'] = station
            hourly_data.reset_index(inplace=True)
            
            results.append(hourly_data)
            
            # Force garbage collection
            gc.collect()
        
        # Combine results
        result = pd.concat(results, ignore_index=True)
        
        # Format date
        result['date'] = result['date'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        
        # Filter date range
        mask = (result['date'] >= start_datetime) & (result['date'] <= end_datetime)
        result = result[mask]
        
        return result
    except Exception as e:
        app.logger.error(f'Error in hourly averaging: {str(e)}')
        raise

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
            <label for="variables">Select variables p2okreset claude:</label><br>
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
        base_url = "http://sensor.aireciudadano.com:30000/api/v1"
        query = '{job%3D"pushgateway"}'
        
        # Get form data
        start_date = request.form['start_date']
        start_time = request.form['start_time']
        end_date = request.form['end_date']
        end_time = request.form['end_time']
        step_number = request.form['step_number']
        step_option = request.form['step_option']
        aggregation_method = request.form['aggregation_method']
        station_filter = request.form.get('station_filter', '')
        
        # Format datetime strings
        start_datetime = f"{start_date}T{start_time}:00Z"
        start_datetime_adjusted = (
            datetime.datetime.fromisoformat(start_datetime[:-1]) - 
            datetime.timedelta(hours=1)
        ).isoformat() + 'Z'
        end_datetime = f"{end_date}T{end_time}:00Z"
        
        # Determine step and construct URL
        if aggregation_method == 'average':
            step = '1m'
            url = f"{base_url}/query_range?query={query}&start={start_datetime_adjusted}&end={end_datetime}&step={step}"
        else:
            step = _get_step(step_number, step_option)
            url = f"{base_url}/query_range?query={query}&start={start_datetime}&end={end_datetime}&step={step}"
        
        # Get and process data
        obs = get_data(url, variables)
        
        # Apply station filter if provided
        if station_filter:
            filters = station_filter.split(',')
            obs = obs[obs['station'].str.contains('|'.join(filters), case=False)]
        
        # Process based on aggregation method
        if aggregation_method == 'average':
            obs = process_hourly_averages(obs, start_datetime, end_datetime)
        else:
            obs['date'] = pd.to_datetime(obs['date'], utc=True)
            mask_start = obs['date'] == pd.to_datetime(start_datetime, utc=True)
            mask_step = (
                (obs['date'] > pd.to_datetime(start_datetime, utc=True)) & 
                (obs['date'] <= pd.to_datetime(end_datetime, utc=True))
            )
            obs = obs[mask_start | mask_step]
        
        # Prepare response data
        total_records = len(obs)
        
        # Convert to records efficiently
        records = obs.replace({np.nan: None}).to_dict('records')
        
        # Group by station
        grouped_data = {}
        for record in records:
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
    app.run(debug=True, host='0.0.0.0', port=8081)
