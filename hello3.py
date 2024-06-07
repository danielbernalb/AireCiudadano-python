import requests
import datetime
import pytz

# Configuración de Prometheus
prometheus_url = 'http://localhost:30000/api/v1'
metrics = ['PM25', 'CO2', 'VOC', 'NOx', 'Humidity', 'Temperature', 'Noise', 'RSSI', 'Latitude', 'Longitude', 'InOut', 'ConfigVal', 'PM25raw', 'NoisePeak', 'PM251', 'PM252', 'PM1', 'MAC', 'Var1', 'Var2']

# Configuración de zona horaria GMT-5
gmt_minus_5 = pytz.timezone('Etc/GMT+5')

# Crear tiempos en GMT-5
start_time_gmt5 = datetime.datetime(2024, 6, 4, 0, 0, 0, tzinfo=gmt_minus_5)
end_time_gmt5 = datetime.datetime(2024, 6, 5, 0, 0, 0, tzinfo=gmt_minus_5)

# Convertir tiempos a UTC
start_time_utc = start_time_gmt5.astimezone(pytz.utc)
end_time_utc = end_time_gmt5.astimezone(pytz.utc)

# Convertir a Unix timestamp
start_time = int(start_time_utc.timestamp())
end_time = int(end_time_utc.timestamp())

step = '1h'

# Configuración de InfluxDB
bucket = "prome1"
org = "AireCiudadano"
token = "8f0FrNt4M0EEZcdO28OpF_H4rUr1822u8cCcIM0tvgoUkYLJbMe-I7T5Ir4cpc8bPjax5vs4_rrWTwE7X6YE0Q=="
url = f"http://localhost:8086/api/v2/write?org={org}&bucket={bucket}&precision=s"

headers = {
    'Authorization': f'Token {token}',
    'Content-Type': 'text/plain; charset=utf-8'
}

# Función para obtener todos los IDs dinámicamente desde Prometheus
def get_all_ids():
    query = 'label_values(id)'
    response = requests.get(f'{prometheus_url}/label/id/values')
    
    try:
        data = response.json()
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e.msg}")
        print(f"Response content: {response.text}")
        return []

    if 'data' in data:
        return data['data']
    else:
        print(f"Error en la consulta de IDs: {response.text}")
        return []

# Función para consultar Prometheus y enviar a InfluxDB
def query_and_send(id, metric):
    query = f'{metric}{{id="{id}"}}'
    params = {
        'query': query,
        'start': start_time,
        'end': end_time,
        'step': step
    }
    response = requests.get(f'{prometheus_url}/query_range', params=params)
    
    try:
        data = response.json()
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e.msg}")
        print(f"Response content: {response.text}")
        return

    if 'data' in data and 'result' in data['data']:
        points = []
        for result in data['data']['result']:
            for value in result['values']:
                timestamp, val = value
                point = f"{metric},id={id} value={val} {int(float(timestamp))}\n"
                points.append(point)
        
        # Enviar datos a InfluxDB
        if points:
            response = requests.post(url, headers=headers, data=''.join(points))
            if response.status_code != 204:
                print(f"Error enviando datos a InfluxDB: {response.text}")
            else:
                print(f"Datos de {metric} para {id} enviados exitosamente a InfluxDB.")
    else:
        print(f"No se encontraron datos para la métrica: {metric} de la estación: {id}")

# Obtener todos los IDs
ids = get_all_ids()

# Consultar y enviar datos para cada métrica de cada estación
for id in ids:
    for metric in metrics:
        query_and_send(id, metric)
