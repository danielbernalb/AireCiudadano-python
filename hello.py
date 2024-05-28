import json
import requests

def transform_data(input_json):
    output_data = []

    # Lista de tipos de medición queremos incluir, en el orden deseado
    desired_measurements = ["PM25", "Temperature", "Humidity", "CO2"]

    for entry in input_json["data"]:
        station = {
            "id": "",
            "station_name": "",
            "scientificName": "AireCiudadano Air quality Station",
            "ownerInstitutionCodeProperty": "AireCiudadano",
            "type": "FixedStation",
            "license": "CC BY-NC-SA 3.0",
            "measurements": [],
            "locationID": "",
            "georeferencedBy": "AireCiudadano firmware",
            "georeferencedDate": "",
            "decimalLatitude": None,
            "decimalLongitude": None,
            "observedOn": ""
        }

        measurements = [] 

        for key, value in entry.items():
            if key == "labels":
                station["id"] = value["job"]
                station["station_name"] = value["job"]
            elif key == "Latitude":
                station["decimalLatitude"] = float(value["metrics"][0]["value"])
            elif key == "Longitude":
                station["decimalLongitude"] = float(value["metrics"][0]["value"])
            elif key in desired_measurements: 
                measurement = {
                    "measurementID": value["time_stamp"],
#                    "measurementType": "PM2.5" if key == "PM25" else key,
                    "measurementType": key,
                    "measurementUnit": get_measurement_unit(key),
                    "measurementDeterminedDate": value["time_stamp"],
                    "measurementDeterminedBy": f"AireCiudadano station {station['station_name']}",
                    "measurementValue": float(value["metrics"][0]["value"])
                }
                measurements.append(measurement)
                station["locationID"] = value["time_stamp"]
                station["observedOn"] = value["time_stamp"]
                station["georeferencedDate"] = value["time_stamp"]

        # Ordena las mediciones según el orden deseado
        station["measurements"] = sorted(measurements, key=lambda m: desired_measurements.index(m["measurementType"]))

        # Cambia "PM25" a "PM2.5" en la salida final
        for measurement in station["measurements"]:
            if measurement["measurementType"] == "PM25":
                measurement["measurementType"] = "PM2.5"
        
        # Solo añadimos la estación si tiene mediciones en desired_measurements
        if station["measurements"]:
            output_data.append(station)

    return output_data

def get_measurement_unit(measurement_type):
    # Define a mapping from measurement type to unit
    unit_mapping = {
        "PM25": "ug/m3",
        "Temperature": "°C",
        "Humidity": "%",
        "CO2": "ppm",
        # Add other mappings as necessary
    }
    return unit_mapping.get(measurement_type, "")

if __name__ == "__main__":
    # Define la URL del JSON de entrada
    url = "http://sensor.aireciudadano.com:30991/api/v1/metrics"  # Reemplaza con la URL real

    # Realiza la solicitud HTTP para obtener el JSON
    response = requests.get(url)
    response.raise_for_status()  # Lanza una excepción si la solicitud falla

    # Carga el JSON de la respuesta
    input_json = response.json()
    #print("Trama de entrada:")
    input_json_dumps = json.dumps(input_json, separators=(',', ':'))
    #print(input_json_dumps)
    #print("")
    #print("")

    # Transforma los datos
    output_json = transform_data(input_json)
    output_json_dumps = json.dumps(output_json, separators=(',', ':'))

    # Guarda el JSON de salida en un archivo
    with open("output.json", "w", encoding='utf-8') as outfile:
        json.dump(output_json, outfile, indent=4, ensure_ascii=False, default=str)
    print("Trama de salida:")
    print(output_json_dumps)
