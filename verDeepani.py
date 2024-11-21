from flask import Flask, request, render_template_string, jsonify, send_file
import os
import json
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from matplotlib.animation import FuncAnimation, FFMpegWriter
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from cartopy.io import img_tiles
import numpy as np
import contextily as ctx

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
# Crear mapa de animación con Cartopy
def create_animation(df, output_path, fps=2, size_scale=2, map_style='osm', zoom=8, center_lat=None, center_lon=None):
    # Determine map extent based on user input or data
    if center_lat is not None and center_lon is not None:
        center = [center_lon, center_lat]
        resolution = 360 / (2 ** zoom)
        half_size_lon = 0.5 * resolution
        half_size_lat = 0.5 * resolution
        extent = [
            center[0] - half_size_lon, center[0] + half_size_lon,
            center[1] - half_size_lat, center[1] + half_size_lat
        ]
    else:
        extent = [
            df["Longitude"].min() - 0.1, df["Longitude"].max() + 0.1,
            df["Latitude"].min() - 0.1, df["Latitude"].max() + 0.1
        ]
    
    # Configuración del mapa con Cartopy
    fig = plt.figure(figsize=(12, 10), dpi=200)  # Tamaño más ajustado
    ax = plt.axes(projection=ccrs.PlateCarree())
    ax.set_extent(extent, crs=ccrs.PlateCarree())
    
    # Add basemap using contextily with a gray background
    ctx.add_basemap(
        ax,
        source="https://cartodb-basemaps-a.global.ssl.fastly.net/light_all/{z}/{x}/{y}.png",
        crs=ccrs.PlateCarree(),
        zoom=zoom
    )
    
    ax.add_feature(cfeature.BORDERS, linestyle=':', edgecolor='black')
    ax.add_feature(cfeature.COASTLINE, edgecolor='black')
    
    # Ajustar márgenes para minimizar espacio blanco
    plt.subplots_adjust(left=0.02, right=0.98, top=0.95, bottom=0.02)
    
    # Preparar los puntos sólidos
    scatter = ax.scatter(
        [], [], s=70, c=[], transform=ccrs.PlateCarree(), alpha=1.0, edgecolors='none'  # Puntos sólidos
    )
    
    # Leyenda de colores en la esquina inferior izquierda
    legend_colors = {
        "green": "0-12 μg/m³ (Bueno)",
        "yellow": "13-34 μg/m³ (Moderado)",
        "orange": "35-54 μg/m³ (Dañino grupos sensibles)",
        "red": "55-149 μg/m³ (Dañino)",
        "purple": "150-249 μg/m³ (Muy daniño)",
        "brown": "250+ μg/m³ (Peligroso)"
    }
    for color, label in legend_colors.items():
        ax.scatter([], [], color=color, label=label, s=180, alpha=1.0)  # Tamaño aumentado
    ax.legend(
        title="Niveles PM2.5",
        loc="lower left",
        fontsize=12,       # Texto más grande
        title_fontsize=14, # Título más grande
        frameon=True,
        facecolor="white",
        edgecolor="black"
    )
    
    # Función de actualización por frame
    def update(frame):
        current_time = sorted(df['date'].unique())[frame]
        data_frame = df[df['date'] == current_time]
        data_frame = data_frame[data_frame['InOut'] == 0.0]
        colors = data_frame["PM25"].apply(pm25_to_color)
        scatter.set_offsets(data_frame[["Longitude", "Latitude"]])
        scatter.set_facecolor(colors)  # Asegurar colores sólidos
        ax.set_title(f"PM2.5 - {current_time.strftime('%Y-%m-%d %H:%M:%S')}", fontsize=20)  # Título más grande
    
    total_frames = len(df['date'].unique())
    
    # Configuración para tiempo adicional en la última frame
    extra_time_frames = fps * 2  # Tiempo adicional de 2 segundos
    total_frames_with_extra = total_frames + extra_time_frames

    def extended_update(frame):
        if frame < total_frames:
            update(frame)
        else:
            update(total_frames - 1)  # Repite la última frame

    ani = FuncAnimation(fig, extended_update, frames=total_frames_with_extra, repeat=False)
    
    writer = FFMpegWriter(fps=fps, metadata=dict(artist='Me'), bitrate=1800)
    ani.save(output_path, writer=writer)

@app.route('/getdata', methods=['GET', 'POST'])
def getdata():
    if request.method == 'POST':
        file = request.files.get('file')
        fps = request.form.get('fps', 2, type=int)
        size_scale = request.form.get('size_scale', 2, type=int)
        map_style = request.form.get('map_style', 'osm')
        zoom = request.form.get('zoom', 8, type=int)
        center_lat = request.form.get('center_lat', type=float)
        center_lon = request.form.get('center_lon', type=float)
        
        if not file:
            return jsonify({"error": "No file uploaded"})
        if not file.filename.endswith('.json'):
            return jsonify({"error": "Only JSON files are allowed"})
        if center_lat is None or center_lon is None:
            return jsonify({"error": "Invalid coordinates"})
        
        save_path = os.path.join(UPLOAD_FOLDER, file.filename)
        file.save(save_path)
        
        with open(save_path, 'r') as f:
            json_data = json.load(f)
        df = create_dataframe(json_data["data"])
        
        output_file = os.path.join(OUTPUT_FOLDER, "pm25_animation.mp4")
        try:
            create_animation(df, output_file, fps=fps, size_scale=size_scale,
                             map_style=map_style, zoom=zoom,
                             center_lat=center_lat, center_lon=center_lon)
        except Exception as e:
            return jsonify({"error": f"Error generating animation: {e}"})
        
        return send_file(output_file, as_attachment=True)
    
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
            
            <label for="map_style">Estilo de mapa:</label><br>
            <select id="map_style" name="map_style">
                <option value="osm">OpenStreetMap</option>
                <option value="satellite">Satellite</option>
                <option value="cartodb">CartoDB</option>
            </select><br><br>
            
            <label for="zoom">Nivel de zoom:</label><br>
            <input type="number" id="zoom" name="zoom" value="8" min="1" max="18" required><br><br>
            
            <label for="center_lat">Latitud central:</label><br>
            <input type="number" id="center_lat" name="center_lat" step="0.0001" required><br><br>
            
            <label for="center_lon">Longitud central:</label><br>
            <input type="number" id="center_lon" name="center_lon" step="0.0001" required><br><br>
            
            <button type="submit">Subir y Configurar</button>
        </form>
    </body>
    </html>
    ''')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8084)