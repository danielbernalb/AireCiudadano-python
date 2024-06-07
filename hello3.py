import requests
import json
import datetime
import pytz

# Configuración de Prometheus
prometheus_url = 'http://localhost:30000/api/v1/query_range'
metrics = ['id', 'PM25', 'PM25raw', 'PM1', 'humidity', 'temperature', 'RSSI', 'latitude', 'longitude', 'inout', 'configval', 'datavar1']

# Configuración de zona horaria GMT-5
gmt_minus_5 = pytz.timezone('Etc/GMT+5')

# Crear tiempos en GMT-5
start_time_gmt5 = datetime.datetime(2021, 1, 1, 0, 0, 0, tzinfo=gmt_minus_5)
end_time_gmt5 = datetime.datetime(2024, 1, 1, 0, 0, 0, tzinfo=gmt_minus_5)

# Convertir tiempos a UTC
start_time_utc = start_time_gmt5.astimezone(pytz.utc)
end_time_utc = end_time_gmt5.astimezone(pytz.utc)

# Formatear tiempos en ISO 8601
start_time = start_time_utc.isoformat()
end_time = end_time_utc.isoformat()

step = '1h'

# Configuración de InfluxDB
bucket = "BucketProme"
org = "AireCiudadano"
token = "RwMO31ubucHoz349BavmHgJcsOOJ__jlBHOAOHx0MJbecAyf7BwsD2ScWOdiX3yuxlTzqXIqQBe1x1fq7H-9mA=="
url = f"http://localhost:8086/api/v2/write?org={org}&bucket={bucket}&precision=s"

headers = {
    'Authorization': f'Token {token}',
    'Content-Type': 'text/plain; charset=utf-8'
}

# Función para consultar Prometheus y enviar a InfluxDB
def query_and_send(metric):
    params = {
        'query': metric,
        'start': start_time,
        'end': end_time,
        'step': step
    }
    response = requests.get(prometheus_url, params=params)
    
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
                point = f"{metric},host=prometheus value={val} {int(float(timestamp))}\n"
                points.append(point)
        
        # Enviar datos a InfluxDB
        if points:
            response = requests.post(url, headers=headers, data=''.join(points))
            if response.status_code != 204:
                print(f"Error enviando datos a InfluxDB: {response.text}")
            else:
                print(f"Datos de {metric} enviados exitosamente a InfluxDB.")
    else:
        print(f"No se encontraron datos para la métrica: {metric}")

# Consultar y enviar datos para cada métrica
for metric in metrics:
    query_and_send(metric)