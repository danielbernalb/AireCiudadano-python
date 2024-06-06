import requests
import json
import datetime

# Configuración de Prometheus
prometheus_url = 'http://localhost:30991/api/v1/query_range'
metrics = ['PM25', 'PM25raw', 'PM1', 'humidity', 'temperature', 'RSSI', 'latitude', 'longitude', 'inout', 'configval', 'datavar1']
start_time = '2024-06-01T00:00:00Z'
end_time = '2024-06-02T00:00:00Z'
step = '60s'

# Configuración de InfluxDB
bucket = "BucketPrometheus"
org = "AireCiudadano"
token = "ozkDDiAMbzXcv3QhiksqowviQ-1GonMSD8hyGZiAes4fk6fKAReWsik12AJsB3HuZBmb3NHiBNr7io-DCNG5Sw"
url = "http://localhost:8086/api/v2/write?org=" + org + "&bucket=" + bucket + "&precision=s"

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

