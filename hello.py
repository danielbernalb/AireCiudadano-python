import json
import requests

def transform_data(input_json):
    output_data = []

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

        for key, value in entry.items():
            if key == "labels":
                station["id"] = value["job"]
                station["station_name"] = value["job"]
            elif key == "Latitude":
                station["decimalLatitude"] = float(value["metrics"][0]["value"])
            elif key == "Longitude":
                station["decimalLongitude"] = float(value["metrics"][0]["value"])
            elif key not in ["labels", "last_push_successful", "push_failure_time_seconds", "push_time_seconds"]:
                measurement = {
                    "measurementID": value["time_stamp"],
                    "measurementType": key,
                    "measurementUnit": get_measurement_unit(key),
                    "measurementDeterminedDate": value["time_stamp"],
                    "measurementDeterminedBy": f"AireCiudadano station {station['station_name']}",
                    "measurementValue": float(value["metrics"][0]["value"])
                }
                station["measurements"].append(measurement)
                station["locationID"] = value["time_stamp"]
                station["observedOn"] = value["time_stamp"]
                station["georeferencedDate"] = value["time_stamp"]

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
    print("Trama de entrada:")
    input_json_dumps = json.dumps(input_json, separators=(',', ':'))
    print(input_json_dumps)
    print("")
    print("")

    # Transforma los datos
    output_json = transform_data(input_json)
    output_json_dumps = json.dumps(output_json, separators=(',', ':'))

    # Guarda el JSON de salida en un archivo
    with open("output.json", "w") as outfile:
        json.dump(output_json_dumps, outfile, indent=4)
    print("Trama de salida:")
    print(output_json_dumps)
