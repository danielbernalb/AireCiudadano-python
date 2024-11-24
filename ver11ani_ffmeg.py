import subprocess  # Importar para usar FFmpeg
from flask import Flask, request, render_template_string, jsonify, send_file
import os
import json
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from matplotlib import font_manager
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

# Función para convertir el video con FFmpeg
def convert_video_to_android_compatible(input_path, output_path):
    """
    Convierte un video MP4 a un formato compatible con WhatsApp Android.
    """
    try:
        # Comando FFmpeg para asegurar compatibilidad con WhatsApp
        subprocess.run([
            "ffmpeg", "-y",  # Sobrescribe el archivo de salida
            "-i", input_path,  # Archivo de entrada
            "-vf", "scale=w=3840:h=2160:force_original_aspect_ratio=decrease",  # Escala a 4K (3840x2160)
            "-c:v", "libx264",  # Codec de video H.264
            "-profile:v", "baseline",  # Perfil baseline para compatibilidad
            "-level", "3.0",  # Nivel de compatibilidad
            "-pix_fmt", "yuv420p",  # Formato de píxel compatible
            "-b:v", "3000k",  # Tasa de bits
            "-movflags", "+faststart",  # Optimiza para streaming
            "-c:a", "aac",  # Codec de audio AAC
            "-b:a", "128k",  # Tasa de bits de audio
            "-ar", "44100",  # Frecuencia de muestreo de audio
            "-shortest",  # Asegura que la duración sea igual al video más corto
            output_path
        ], check=True)
        return True
    except subprocess.CalledProcessError as e:
        app.logger.error(f"FFmpeg error: {e}")
        return False
    
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

def create_dataframe(json_data):
    records = []
    for station, entries in json_data.items():
        for entry in entries:
            entry["station"] = station
            records.append(entry)
    df = pd.DataFrame(records)
    df['date'] = pd.to_datetime(df['date'])
    return df

def create_animation(df, output_path, fps=2, size_scale=2, map_style='osm', zoom=10, zoom_base=12, center_lat=4.6257, center_lon=-74.1340):
    center = [center_lon, center_lat]
    resolution = 360 / (2 ** zoom)
    half_size_lon = 0.5 * resolution
    half_size_lat = 0.5 * resolution
    extent = [
        center[0] - half_size_lon, center[0] + half_size_lon,
        center[1] - half_size_lat, center[1] + half_size_lat
    ]

    tile_sources = {
        'osm': ctx.providers.OpenStreetMap.Mapnik,
        'cartodb': ctx.providers.CartoDB.Positron,
        'cartodb_dark': ctx.providers.CartoDB.DarkMatter,
        'satellite': ctx.providers.Esri.WorldImagery,
    }

    tile_source = tile_sources.get(map_style, ctx.providers.OpenStreetMap.Mapnik)

    # Figura con relación de aspecto 1:1
    fig = plt.figure(figsize=(10, 10), dpi=300)
    ax = plt.axes(projection=ccrs.PlateCarree())
    ax.set_extent(extent, crs=ccrs.PlateCarree())

    ctx.add_basemap(ax, source=tile_source, crs=ccrs.PlateCarree(), zoom=zoom_base)

    ax.add_feature(cfeature.BORDERS, linestyle=':', edgecolor='black')
    ax.add_feature(cfeature.COASTLINE, edgecolor='black')

    # Ajustar márgenes: reduce márgenes laterales
    plt.subplots_adjust(left=0.02, right=0.99, top=0.95, bottom=0.02)

    scatter = ax.scatter(
        [], [], s=70 * size_scale, transform=ccrs.PlateCarree(),
        alpha=1.0, linewidths=0.8
    )

    legend_colors = {
        "green": "0-12 (Bueno)",
        "yellow": "13-34 (Moderado)",
        "orange": "35-54 (Dañino grupos sensibles)",
        "red": "55-149 (Dañino)",
        "purple": "150-249 (Muy daniño)",
        "brown": "250+ (Peligroso)"
    }
    for color, label in legend_colors.items():
        ax.scatter([], [], color=color, label=label, s=180, alpha=1.0)

    bold_font = font_manager.FontProperties(weight="bold", size=12)
    bold_title_font = font_manager.FontProperties(weight="bold", size=15)
    legend = ax.legend(
        title="Niveles PM2.5 (μg/m³)",
        loc="lower left",
        frameon=True,
        facecolor="white",
        edgecolor="black",
        markerscale=1.1,  # Aumenta tamaño de íconos
        prop=bold_font
    )
    legend.get_title().set_fontproperties(bold_title_font)

    # Función de actualización de los frames
    def update(frame):
        current_time = sorted(df['date'].unique())[frame]
        data_frame = df[df['date'] == current_time]
        data_frame = data_frame[data_frame['InOut'] == 0.0]
        
        colors = data_frame["PM25"].apply(pm25_to_color)
        scatter.set_offsets(data_frame[["Longitude", "Latitude"]])
        scatter.set_facecolor(colors)
        scatter.set_edgecolor(colors)
        ax.set_title(f"Red AireCiudadano - {current_time.strftime('%Y-%m-%d %H:%M:%S')}", fontsize=24, fontweight="bold")

    total_frames = len(df['date'].unique())
    extra_time_frames = int(max(2 / fps, 2))
    total_frames_with_extra = total_frames + extra_time_frames

    def extended_update(frame):
        if frame < total_frames:
            update(frame)
        else:
            update(total_frames - 1)

    ani = FuncAnimation(fig, extended_update, frames=total_frames_with_extra, repeat=False, blit=False)
    writer = FFMpegWriter(fps=fps, metadata=dict(artist='Me'), bitrate=1800)
    ani.save(output_path, writer=writer)

@app.route('/getdata', methods=['GET', 'POST'])
def getdata():
    if request.method == 'POST':
        file = request.files.get('file')
        fps = request.form.get('fps', 2, type=float)
        size_scale = request.form.get('size_scale', 2.0, type=float)
        map_style = request.form.get('map_style', 'osm')
        zoom = request.form.get('zoom', 10, type=int)
        zoom_base = request.form.get('zoom_base', 12, type=int)
        center_lat = request.form.get('center_lat', 4.6257, type=float)
        center_lon = request.form.get('center_lon', -74.15, type=float)

        if not file:
            return jsonify({"error": "No file uploaded"})
        if not file.filename.endswith('.json'):
            return jsonify({"error": "Only JSON files are allowed"})
        
        save_path = os.path.join(UPLOAD_FOLDER, file.filename)
        file.save(save_path)

        with open(save_path, 'r') as f:
            json_data = json.load(f)
        df = create_dataframe(json_data["data"])
        
        output_file = os.path.join(OUTPUT_FOLDER, "pm25_animation.mp4")
        compatible_output_file = os.path.join(OUTPUT_FOLDER, "pm25_animation_android.mp4")

        try:
            create_animation(df, output_file, fps=fps, size_scale=size_scale,
                             map_style=map_style, zoom=zoom, zoom_base=zoom_base,
                             center_lat=center_lat, center_lon=center_lon)
            
            # Convertir el video generado a un formato compatible con Android y WhatsApp
            if not convert_video_to_android_compatible(output_file, compatible_output_file):
                return jsonify({"error": "Error converting video for Android compatibility"})
            
        except Exception as e:
            return jsonify({"error": f"Error generating animation: {e}"})
        
        # Retorna el archivo convertido
        return send_file(compatible_output_file, as_attachment=True)

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
            <input type="number" id="fps" name="fps" value="2" step="0.1" min="0.1" max="60" required><br><br>
            
            <label for="size_scale">Tamaño de puntos (escala):</label><br>
            <input type="number" id="size_scale" name="size_scale" value="2" step="0.1" min="0.1" max="100" required><br><br>
            
            <label for="map_style">Estilo de mapa:</label><br>
            <select id="map_style" name="map_style">
                <option value="osm">OpenStreetMap</option>
                <option value="cartodb">CartoDB Positron (Claro)</option>
                <option value="cartodb_dark">CartoDB Dark Matter (Oscuro)</option>
                <option value="satellite">Esri Satélite</option>
            </select><br><br>
            
            <label for="zoom">Nivel de zoom general (1-18):</label><br>
            <input type="number" id="zoom" name="zoom" value="10" min="1" max="18" required><br><br>
            
            <label for="zoom_base">Nivel de zoom del mapa base (1-18):</label><br>
            <input type="number" id="zoom_base" name="zoom_base" value="12" min="1" max="18" required><br><br>
            
            <label for="center_lat">Latitud central (Por defecto: Bogotá):</label><br>
            <input type="number" id="center_lat" name="center_lat" value="4.62" step="0.0001" required><br><br>
            
            <label for="center_lon">Longitud central (Por defecto: Bogotá):</label><br>
            <input type="number" id="center_lon" name="center_lon" value="-74.15" step="0.0001" required><br><br>
            
            <button type="submit">Subir archivo y generar animación</button>
        </form>
    </body>
    </html>
    ''')

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=8084)
