# Version de APIdata para video. 26nov2024
# Resultado en la hora GMT escogida, ejemplo si es de 8:00am a 10:00am con GMT-5, el resultado es la consulta de 3:00am a 5:00am con GMT

from flask import Flask, request, jsonify, render_template_string, send_file, Response
import requests
import pandas as pd
import datetime
import numpy as np
import time
import io
import json
import pytz
import logging

selected_cols = [
    "PM25", "PM25raw", "Humidity", "Temperature", "ConfigVal", "Latitude", "Longitude", "InOut",
]

app = Flask(__name__)
app.logger.setLevel(logging.DEBUG)

def process_data_in_chunks(url, variables, start_datetime, end_datetime, interval_minutes):
    chunk_size = datetime.timedelta(days=7)
    current_start = start_datetime
    all_data = []

    while current_start < end_datetime:
        current_end = min(current_start + chunk_size, end_datetime)
        # Usar step de 1 minuto
        chunk_data = get_data(url, variables, current_start, current_end, '1m', interval_minutes=interval_minutes)

        # Aplicar promedio horario al chunk
        chunk_data['date'] = pd.to_datetime(chunk_data['date'], utc=True)
        chunk_data.set_index(['station', 'date'], inplace=True)
        chunk_data = chunk_data.apply(pd.to_numeric, errors='coerce')

        interval_chunks = []
        for station, group in chunk_data.groupby('station'):
            interval_str = f'{interval_minutes}T'
            interval_data = group.resample(interval_str, level='date').mean()
            interval_data.index = interval_data.index + pd.Timedelta(minutes=interval_minutes)
            interval_data['station'] = station
            interval_chunks.append(interval_data.reset_index())

        chunk_data = pd.concat(interval_chunks, ignore_index=True)
        all_data.append(chunk_data)

        current_start = current_end

        progress = {
            'current_date': current_end.isoformat(),
            'progress_percentage': min(100, (current_end - start_datetime) / (end_datetime - start_datetime) * 100)
        }
        yield progress, chunk_data

    return all_data

# Get data from API with time intervals
def get_data(url, selected_cols, start_datetime, end_datetime, step, interval_minutes):
    all_results = []
    current_start_time = start_datetime

    while current_start_time < end_datetime:
        current_end_time = min(current_start_time + datetime.timedelta(minutes=interval_minutes), end_datetime)
        current_end_time_1s = current_end_time - pd.Timedelta(seconds=1)
        query_url = f"{url}&start={current_start_time.isoformat()}Z&end={current_end_time_1s.isoformat()}Z&step={step}"

        app.logger.debug(f"Querying data from {current_start_time} to {current_end_time_1s}")

        try:
            response = requests.get(query_url)
            response.raise_for_status()
            data = response.json().get('data', {}).get('result', [])

            if not data:
                app.logger.warning(f"No data returned from API for interval {current_start_time} to {current_end_time}")
                current_start_time = current_end_time
                continue

            df = pd.json_normalize(data)

            required_columns = ['metric.__name__', 'metric.exported_job', 'values'] if 'values' in df.columns else ['metric.__name__', 'metric.exported_job', 'value']
            if not all(col in df.columns for col in required_columns):
                app.logger.warning(f"Missing required columns for interval {current_start_time} to {current_end_time}")
                current_start_time = current_end_time
                continue

            if 'values' in df.columns:
                df = df.explode('values')
                df['date'] = df['values'].apply(lambda x: datetime.datetime.fromtimestamp(x[0], datetime.timezone.utc).isoformat())
                df['value'] = df['values'].apply(lambda x: x[1])
                df = df.drop(columns="values")
            elif 'value' in df.columns:
                df['date'] = df['value'].apply(lambda x: datetime.datetime.fromtimestamp(x[0], datetime.timezone.utc).isoformat())
                df['value'] = df['value'].apply(lambda x: x[1])

            df = df.rename(columns={"metric.__name__": "metric_name", "metric.exported_job": "station"})

            if 'station' not in df.columns:
                app.logger.warning(f"Missing 'station' column after renaming for interval {current_start_time} to {current_end_time}")
                current_start_time = current_end_time
                continue

            df = df[df['station'].notnull()]

            if df.empty:
                app.logger.warning(f"No valid data after filtering for interval {current_start_time} to {current_end_time}")
                current_start_time = current_end_time
                continue

            try:
                df_result = _wide_table(df, selected_cols)
            except Exception as pivot_error:
                app.logger.warning(f"Error in pivot operation: {str(pivot_error)} for interval {current_start_time} to {current_end_time}")
                current_start_time = current_end_time
                continue

            for col in selected_cols:
                if col in df_result.columns:
                    df_result[col] = df_result[col].astype(float)
            if 'Latitude' in df_result.columns:
                df_result['Latitude'] = df_result['Latitude'].replace(0, np.nan)
            if 'Longitude' in df_result.columns:
                df_result['Longitude'] = df_result['Longitude'].replace(0, np.nan)

            all_results.append(df_result)

        except requests.exceptions.RequestException as e:
            app.logger.error(f'Network error fetching data chunk: {str(e)}')
            raise
        except Exception as e:
            app.logger.error(f'Error processing data chunk: {str(e)}')
            current_start_time = current_end_time
            continue

        current_start_time = current_end_time

    if not all_results:
        app.logger.warning("No valid data found for entire time range")
        columns = ['station', 'date'] + selected_cols
        return pd.DataFrame(columns=columns)

    final_df = pd.concat(all_results, ignore_index=True)
    app.logger.debug(f"Final dataframe shape after concatenation: {final_df.shape}")
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


@app.route('/getdata')
def index():
    variables = request.args.getlist('variables') or selected_cols
    start_date = request.args.get('start_date', '2024-05-09')
    start_time = request.args.get('start_time', '08:00')
    end_date = request.args.get('end_date', '2024-05-09')
    end_time = request.args.get('end_time', '10:00')
    station_filter = request.args.get('station_filter', '')
    exclude_stations = request.args.get('exclude_stations', '')
    interval_minutes = request.args.get('interval_minutes', 60)
    gmt_offset = request.args.get('gmt_offset', 0)

    return render_template_string('''
        <form action="/dataresult" method="post">
            <h2>API AIRECIUDADANO video</h2>
            <h3>Instructions at: <a href="https://aireciudadano.com/apidata/" target="_blank">aireciudadano.com/apidata</a></h3><br>
            <label for="variables">Select variables:</label><br>
            <br>
            {% for col in selected_cols %}
                <input type="checkbox" id="{{ col }}" name="variables" value="{{ col }}" {% if col in variables %}checked{% endif %}>
                <label for="{{ col }}">{{ col }}</label><br>
            {% endfor %}
            <br>
            <label for="start_date">Start date/time:</label>
            <input type="date" id="start_date" name="start_date" value="{{ start_date }}">
            <label for="start_time"> / </label>
            <input type="time" id="start_time" name="start_time" value="{{ start_time }}" step="60"><br><br>
            <label for="end_date">End date/time:</label>
            <input type="date" id="end_date" name="end_date" value="{{ end_date }}">
            <label for="end_time"> / </label>
            <input type="time" id="end_time" name="end_time" value="{{ end_time }}" step="60"><br><br>
            <label for="interval_minutes">Interval (minutes):</label>
            <input type="number" id="interval_minutes" name="interval_minutes" value="{{ interval_minutes }}" min="5"><br><br>
            <label for="station_filter">Station Filter (include):</label>
            <input type="text" id="station_filter" name="station_filter" value="{{ station_filter }}"><br><br>
            <label for="exclude_stations">Station Filter (exclude):</label>
            <input type="text" id="exclude_stations" name="exclude_stations" value="{{ exclude_stations }}"><br><br>
            <label for="gmt_offset">Select GMT offset:</label>
            <select id="gmt_offset" name="gmt_offset">
                {% for i in range(-12, 13) %}
                    <option value="{{ i }}" {% if i == gmt_offset|int %}selected{% endif %}>GMT{{ '+' if i >= 0 else '' }}{{ i }}</option>
                {% endfor %}
            </select><br><br>
            <input type="submit" value="Submit">
        </form>
    ''', selected_cols=selected_cols, variables=variables, start_date=start_date,
       start_time=start_time, end_date=end_date, end_time=end_time,
       interval_minutes=interval_minutes, station_filter=station_filter,
       exclude_stations=exclude_stations, gmt_offset=gmt_offset)

@app.route('/dataresult', methods=['POST'])
def data():
    start_time = time.time()

    try:
        variables = request.form.getlist('variables')
        base_url = "http://sensor.aireciudadano.com:30000/api/v1"
        query = '{job%3D"pushgateway"}'

        start_date = request.form['start_date']
        start_time_str = request.form['start_time']
        end_date = request.form['end_date']
        end_time = request.form['end_time']
        station_filter = request.form.get('station_filter', '')
        exclude_stations = request.form.get('exclude_stations', '')  # Nuevo campo para excluir estaciones
        interval_minutes = int(request.form.get('interval_minutes', 60))
        gmt_offset = int(request.form.get('gmt_offset', 0))  # GMT seleccionado

        if interval_minutes < 5:
            return jsonify({'error': 'Interval must be at least 5 minutes'})
        
        # Ajustar las horas según el GMT seleccionado
        start_datetime = datetime.datetime.fromisoformat(f"{start_date}T{start_time_str}") - datetime.timedelta(hours=gmt_offset)
        end_datetime = datetime.datetime.fromisoformat(f"{end_date}T{end_time}") - datetime.timedelta(hours=gmt_offset)
        date_diff = end_datetime - start_datetime + datetime.timedelta(minutes=60)

        if date_diff.days > 7:
            all_data = []
            for progress, chunk_data in process_data_in_chunks(f"{base_url}/query_range?query={query}", variables, start_datetime, end_datetime, interval_minutes):
                if station_filter:
                    filters = station_filter.split(',')
                    chunk_data = chunk_data[chunk_data['station'].str.contains('|'.join(filters), case=False)]
                if exclude_stations:  # Aplicar exclusión de estaciones
                    excludes = exclude_stations.split(',')
                    chunk_data = chunk_data[~chunk_data['station'].str.contains('|'.join(excludes), case=False)]
                all_data.append(chunk_data)
            obs = pd.concat(all_data, ignore_index=True)
        else:
            obs = get_data(f"{base_url}/query_range?query={query}", variables, start_datetime, end_datetime, '1m', interval_minutes=interval_minutes)

            obs['date'] = pd.to_datetime(obs['date'], utc=True)
            obs.set_index(['station', 'date'], inplace=True)
            obs = obs.apply(pd.to_numeric, errors='coerce')

            averaged_data = []
            for station, group in obs.groupby('station'):
                averaged = group.resample(f'{interval_minutes}min', level='date').mean()
                averaged['station'] = station
                averaged.reset_index(inplace=True)
                averaged_data.append(averaged)

            obs = pd.concat(averaged_data, ignore_index=True)

            if station_filter:
                app.logger.debug(f"station_filter routine")
                filters = station_filter.split(',')
                obs = obs[obs['station'].str.contains('|'.join(filters), case=False)]
            if exclude_stations:  # Aplicar exclusión de estaciones
                app.logger.debug(f"exclude_stations routine")
                excludes = exclude_stations.split(',')
                obs = obs[~obs['station'].str.contains('|'.join(excludes), case=False)]

       # Ajustar la fecha/hora a formato UTC y filtrar por rango ajustado
        start_datetime = pd.to_datetime(start_datetime).tz_localize('UTC') + pd.Timedelta(hours=gmt_offset)
        end_datetime = pd.to_datetime(end_datetime).tz_localize('UTC') + pd.Timedelta(hours=gmt_offset)

        # Aseguramos que obs['date'] también esté en UTC
        if obs['date'].dtype == 'object':  # Si es texto, convertir a datetime
            obs['date'] = pd.to_datetime(obs['date'])
            app.logger.debug(f"Si es texto, convertir a datetime")

        if obs['date'].dt.tz is None:  # Si no tiene zona horaria, asignar UTC
            obs['date'] = obs['date'].dt.tz_localize('UTC')
            app.logger.debug(f"Si no tiene zona horaria, asignar UTC")

        obs['date'] = obs['date'] + pd.Timedelta(hours=gmt_offset)
        
        obs = obs.round(3)


        # Ajustar formato de fecha para visualización
        obs['date'] = obs['date'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
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

        # Guardar en un archivo JSON
        try:
            filename = f"data_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(filename, 'w') as f:
                json.dump({'data': grouped_data}, f, indent=4)

            return jsonify({
                'message': 'Data successfully saved to JSON file',
                'filename': filename
            })
        except Exception as e:
            app.logger.error(f'Error creating json file: {str(e)}')
            return jsonify({'error': f'Error creating json file: {str(e)}'})

    except Exception as e:
        app.logger.error(f'Error in data endpoint: {str(e)}')
        return jsonify({'error': str(e)})

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8082)