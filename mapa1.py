from flask import Flask, request, render_template_string, jsonify, send_file
import os
import json
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import cartopy.crs as ccrs  # Cartopy para mapas geográficos
import cartopy.feature as cfeature  # Detalles adicionales del mapa
from cartopy.io.img_tiles import OSM  # Fondo de OpenStreetMap
import numpy as np

app = Flask(__name__)
app.logger.setLevel("DEBUG")

UPLOAD_FOLDER = "uploads"
OUTPUT_FOLDER = "output"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# Escala de colores basada en PM25
def pm25_to_color(pm25):
    if pm25 < 13:
        return "green"
    elif 13 <= pm25 < 35:
        return "yellow"
    elif 35 <= pm25 < 55:
        return "orange"
    elif 55 <= pm25 < 150:
        return "red"
    elif 150 <= pm25 < 250:
        return "purple"
    else:
        return "brown"

# Crear DataFrame consolidado para animación
def create_dataframe(json_data):
    records = []
    for station, entries in json_data.items():
        for entry in entries:
            entry["station"] = station
            records.append(entry)
    df = pd.DataFrame(records)
    df['date'] = pd.to_datetime(df['date'])
    return df

# Crear mapa de animación con Cartopy
def create_animation(df, output_path, fps=2, size_scale=2):
    # Configuración del mapa con Cartopy
    fig = plt.figure(figsize=(12, 10))
    ax = plt.axes(projection=ccrs.PlateCarree())  # Proyección geográfica
    ax.set_extent([
        df["Longitude"].min() - 0.1, df["Longitude"].max() + 0.1,
        df["Latitude"].min() - 0.1, df["Latitude"].max() + 0.1
    ], crs=ccrs.PlateCarree())

    # Fondo actualizado con OpenStreetMap
    tile = OSM()  # Fondo basado en OpenStreetMap
    ax.add_image(tile, 8)
    ax.add_feature(cfeature.BORDERS, linestyle=':', edgecolor='black')
    ax.add_feature(cfeature.COASTLINE, edgecolor='black')

    # Preparar los puntos
    scatter = ax.scatter([], [], s=[], c=[], transform=ccrs.PlateCarree(), alpha=0.7)

    # Función de actualización por frame
    def update(frame):
        # Filtrar datos para el frame actual
        current_time = sorted(df['date'].unique())[frame]
        data_frame = df[df['date'] == current_time]

        # Filtrar puntos con InOut = 1
        data_frame = data_frame[data_frame['InOut'] == 0.0]

        sizes = data_frame["PM25"] * size_scale  # Tamaño proporcional al PM2.5
        colors = data_frame["PM25"].apply(pm25_to_color)
        scatter.set_offsets(data_frame[["Longitude", "Latitude"]])
        scatter.set_sizes(sizes)
        scatter.set_color(colors)

        # Actualizar el título con la fecha y hora
        ax.set_title(f"PM2.5 Animación - {current_time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Crear la animación
    total_frames = len(df['date'].unique())
    ani = FuncAnimation(fig, update, frames=total_frames, repeat=False)

    # Guardar como video
    ani.save(output_path, writer="ffmpeg", fps=fps)

@app.route('/getdata', methods=['GET', 'POST'])
def getdata():
    if request.method == 'POST':
        # Procesar los datos del formulario
        file = request.files.get('file')
        fps = request.form.get('fps', 2, type=int)
        size_scale = request.form.get('size_scale', 2, type=int)

        # Validar entrada
        if not file:
            return jsonify({"error": "No file uploaded"})
        if not file.filename.endswith('.json'):
            return jsonify({"error": "Only JSON files are allowed"})

        # Guardar archivo subido
        save_path = os.path.join(UPLOAD_FOLDER, file.filename)
        file.save(save_path)

        # Cargar y procesar JSON
        with open(save_path, 'r') as f:
            json_data = json.load(f)
        df = create_dataframe(json_data["data"])

        # Generar animación
        output_file = os.path.join(OUTPUT_FOLDER, "pm25_animation.mp4")
        try:
            create_animation(df, output_file, fps=fps, size_scale=size_scale)
        except Exception as e:
            return jsonify({"error": f"Error generating animation: {e}"})

        # Enviar el archivo de video como respuesta
        return send_file(output_file, as_attachment=True)

    # Renderizar formulario HTML
    return render_template_string('''
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>PM2.5 Animation Data Upload</title>
    </head>
    <body>
        <h2>Sube tu archivo JSON para animación de PM2.5</h2>
        <form action="/getdata" method="post" enctype="multipart/form-data">
            <label for="file">Seleccionar archivo JSON:</label><br>
            <input type="file" id="file" name="file" accept=".json" required><br><br>
            
            <label for="fps">Velocidad de animación (FPS):</label><br>
            <input type="number" id="fps" name="fps" value="2" min="1" max="60" required><br><br>
            
            <label for="size_scale">Tamaño de puntos (escala):</label><br>
            <input type="number" id="size_scale" name="size_scale" value="2" min="1" max="10" required><br><br>
            
            <button type="submit">Subir y Configurar</button>
        </form>
    </body>
    </html>
    ''')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8083)
