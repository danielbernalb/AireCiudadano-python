import requests
import datetime
import pytz
import json

# Configuración de Prometheus
prometheus_url = 'http://localhost:30000/api/v1'
metrics = ['PM25', 'CO2', 'VOC', 'NOx', 'Humidity', 'Temperature', 'Noise', 'RSSI', 'Latitude', 'Longitude', 'InOut', 'ConfigVal', 'PM25raw', 'NoisePeak', 'PM251', 'PM252', 'PM1', 'MAC', 'Var1', 'Var2']

# Configuración de zona horaria GMT-5
gmt_minus_5 = pytz.timezone('Etc/GMT+5')

# Crear tiempos en GMT-5
start_time_gmt5 = datetime.datetime(2024, 5, 20, 0, 0, 0, tzinfo=gmt_minus_5)
end_time_gmt5 = datetime.datetime(2024, 5, 21, 0, 0, 0, tzinfo=gmt_minus_5)

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
token = "JkU8FABHetPcX1vgxpGmclVjU3Cd7sT5uKHtO0GERqTEIEP8Df9h2UkFHVYKxtGpvWPUb2Vzxsf_cmKGIb41HQ=="
url = f"http://localhost:8086/api/v2/write?org={org}&bucket={bucket}&precision=s"

headers = {
    'Authorization': f'Token {token}',
    'Content-Type': 'text/plain; charset=utf-8'
}

# Función para obtener todos los valores de exported_job desde Prometheus
def get_all_exported_jobs():
    response = requests.get(f'{prometheus_url}/label/exported_job/values')
    
    if response.status_code != 200:
        print(f"Error HTTP al obtener exported_jobs: {response.status_code} {response.reason}")
        return []

    try:
        data = response.json()
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e.msg}")
        return []

    if 'data' in data:
        exported_jobs = data['data']
        return exported_jobs
    else:
        print(f"Error en la consulta de exported_jobs: {response.text}")
        return []

# Función para verificar si un exported_job tiene datos en el rango de tiempo especificado
def check_exported_job_active(exported_job):
    query = f'PM25{{exported_job="{exported_job}"}}'
    params = {
        'query': query,
        'start': start_time,
        'end': end_time,
        'step': step
    }
    response = requests.get(f'{prometheus_url}/query_range', params=params)
    
    if response.status_code != 200:
        print(f"Error HTTP al verificar métrica PM25 para el exported_job {exported_job}: {response.status_code} {response.reason}")
        return False

    try:
        data = response.json()
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e.msg}")
        return False

    if 'data' in data and 'result' in data['data'] and len(data['data']['result']) > 0:
        return True
    else:
        return False

# Función para consultar Prometheus y enviar a InfluxDB
def query_and_send(exported_job, metric):
    query = f'{metric}{{exported_job="{exported_job}"}}'
    params = {
        'query': query,
        'start': start_time,
        'end': end_time,
        'step': step
    }
    response = requests.get(f'{prometheus_url}/query_range', params=params)
    
    if response.status_code != 200:
        print(f"Error HTTP al consultar métrica {metric} para el exported_job {exported_job}: {response.status_code} {response.reason}")
        return

    try:
        data = response.json()
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e.msg}")
        return

    if 'data' in data and 'result' in data['data']:
        points = []
        for result in data['data']['result']:
            for value in result['values']:
                timestamp, val = value
                # Escapar espacios en el exported_job
                sanitized_exported_job = exported_job.replace(" ", "\\ ")
                point = f"{metric},exported_job={sanitized_exported_job} value={val} {int(float(timestamp))}\n"
                points.append(point)
        
        # Enviar datos a InfluxDB
        if points:
            response = requests.post(url, headers=headers, data=''.join(points))
            if response.status_code != 204:
                print(f"Error enviando datos a InfluxDB para {metric} de {exported_job}: {response.status_code} {response.reason}")
            else:
                print(f"Datos de {metric} para {exported_job} enviados exitosamente a InfluxDB.")
    else:
        print(f"No se encontraron datos para la métrica {metric} del exported_job {exported_job}")

# Obtener todos los exported_jobs
exported_jobs = get_all_exported_jobs()

# Verificar si se obtuvieron exported_jobs
if not exported_jobs:
    print("No se encontraron exported_jobs. Terminando el script.")
    exit(1)

# Filtrar exported_jobs que contienen "prueba", "Prueba", "test" o "Test"
filtered_exported_jobs = [job for job in exported_jobs if not any(word in job.lower() for word in ['prueba', 'test'])]

# Filtrar exported_jobs que tienen datos en el rango de tiempo especificado
active_exported_jobs = []
for job in filtered_exported_jobs:
    if check_exported_job_active(job):
        active_exported_jobs.append(job)

print(f"exported_jobs activos en el rango de tiempo: {active_exported_jobs}")

# Consultar y enviar datos para cada métrica de cada exported_job activo
for exported_job in active_exported_jobs:
    for metric in metrics:
        query_and_send(exported_job, metric)
