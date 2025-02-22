# p1okreset_01: Codigo OK en todo, modificado con 01-mini

from flask import Flask, request, jsonify, render_template_string
import requests
import pandas as pd
import datetime
import numpy as np
import json
import dask.dataframe as dd  # Importar Dask para procesamiento en paralelo

# Constants
selected_cols = [
    "PM25", "PM25raw", "PM1", "Humidity", "Temperature",
]

# Aplicación Flask
app = Flask(__name__)

# Función para obtener datos de la API
def get_data(url, selected_cols):
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
                df_result[col] = df_result[col].astype('float32')  # Optimización de tipo de datos
        if 'Latitude' in df_result.columns:
            df_result['Latitude'].replace(0, np.nan, inplace=True)
        if 'Longitude' in df_result.columns:
            df_result['Longitude'].replace(0, np.nan, inplace=True)

        return df_result
    except Exception as e:
        app.logger.error(f'Error in get_data: {str(e)}')
        raise

# Función para obtener tabla en formato ancho
def _wide_table(df, selected_cols):
    try:
        df_result = pd.pivot(df, index=['station', 'date'], columns='metric_name', values='value').reset_index()
        all_cols = ['station', 'date'] + selected_cols
        missing_cols = set(all_cols) - set(df_result.columns)
        for col in missing_cols:
            df_result[col] = np.nan
        df_result = df_result[all_cols].reset_index(drop=True)
        df_result.columns.name = ""
        df_result['station'] = df_result['station'].astype('category')  # Optimización de tipo de datos
        return df_result
    except Exception as e:
        app.logger.error(f'Pivot Error: {str(e)}')
        raise

# Constructor del valor de paso para consultas de rango de tiempo
def _get_step(number, choice):
    options = {"minutes": "m", "hours": "h", "days": "d", "weeks": "w", "years": "y"}
    return f"{number}{options[choice]}"

# Función para remuestreo por hora usando Pandas dentro de Dask
def resample_hourly(df):
    if df.empty:  # Verificar si el DataFrame está vacío
        return pd.DataFrame(columns=['date', 'station'] + selected_cols)  # Devuelve un DataFrame vacío con las columnas necesarias
    
    if 'station' not in df.columns:
        raise ValueError("La columna 'station' no está presente en el DataFrame.")  # Añadir mensaje claro para 'station'

    station = df['station'].iloc[0]  # Guardar la estación para reutilizar
    df = df.set_index('date')
    df = df.resample('1h').mean()
    df['station'] = station  # Volver a asignar la estación después de remuestreo
    for col in selected_cols:  # Asegurar que todas las columnas existan
        if col not in df.columns:
            df[col] = np.nan
    return df.reset_index()

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
            <label for="variables">Select variables 1:</label><br>
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
    base_url = "http://sensor.aireciudadano.com:30000/api/v1"
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
        url = f"{base_url}/query_range?query={query}&start={start_datetime_adjusted}&end={end_datetime}&step={step}"
    else:
        step = _get_step(step_number, step_option)
        url = f"{base_url}/query_range?query={query}&start={start_datetime}&end={end_datetime}&step={step}"

    try:
        obs = get_data(url, variables)
        if station_filter:
            filters = station_filter.split(',')
            obs = obs[obs['station'].str.contains('|'.join(filters), case=False)]

        if aggregation_method == 'step':
            obs['date'] = pd.to_datetime(obs['date'], utc=True)
            mask_start = obs['date'] == pd.to_datetime(start_datetime, utc=True)
            mask_step = (obs['date'] > pd.to_datetime(start_datetime, utc=True)) & (obs['date'] <= pd.to_datetime(end_datetime, utc=True))
            obs = obs[mask_start | mask_step]

        elif aggregation_method == 'average':
            obs['date'] = pd.to_datetime(obs['date'], utc=True)
            ddf = dd.from_pandas(obs, npartitions=4)  # Ajusta particiones según tus cores de CPU
            
            # Agrupación y remuestreo usando map_partitions en Dask
            meta = {'date': 'datetime64[ns, UTC]', 'station': 'category', **{col: 'float32' for col in selected_cols}}
            hourly_obs = ddf.groupby('station', observed=False).apply(lambda df: resample_hourly(df), meta=meta).compute()
            hourly_obs = hourly_obs.reset_index(drop=True)
            hourly_obs['date'] = hourly_obs['date'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
            
            obs = hourly_obs


        obs = obs[(obs['date'] >= start_datetime) & (obs['date'] <= end_datetime)]
        total_records = obs.shape[0]

        # Convertir DataFrame a diccionario
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
    app.run(debug=True, host='0.0.0.0', port=8081)
