# Agregar opciones como:
# 1. eliminar sensores con defectos, Va en la API data
# 3. ajustes a sensores Sensirion SPS30, Plantower, va en la API data
# 5. Probable union de APIdata con APIvideo

import os
import json
import subprocess  # Importar para usar FFmpeg
from flask import Flask, request, render_template_string, jsonify, send_file
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from matplotlib import font_manager
from matplotlib.animation import FuncAnimation, FFMpegWriter
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import contextily as ctx

app = Flask(__name__)
app.logger.setLevel("DEBUG")

UPLOAD_FOLDER = "uploads"
OUTPUT_FOLDER = "output"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

def convert_video_to_android_compatible(input_path, output_path):
    try:
        subprocess.run([
            "ffmpeg", "-y",
            "-loglevel", "warning",
            "-i", input_path,
            "-vf", "scale=-1:1440",  # Scale height to 1440p, width adjusts accordingly
            "-c:v", "libx264",
            "-profile:v", "baseline",
            "-level", "3.0",
            "-pix_fmt", "yuv420p",
            "-b:v", "1000k",
            "-movflags", "+faststart",
            "-c:a", "aac",
            "-b:a", "128k",
            "-ar", "44100",
            "-shortest",
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
    estaciones_ajuste_pm25 = {
        "AireCiudadano_CO_BO_Hayuelos_6d4d48", 
        "AireCiudadano_Cindesus_17611c", 
        "Abago_EduardoSantos_ce0eb0"
    }
    
    for station, entries in json_data.items():
        for entry in entries:
            # Comprobar si ConfigVal está presente y es divisible por 4 sin residuo
            if "ConfigVal" in entry:
                config_val = entry["ConfigVal"]
                if isinstance(config_val, (int, float)) and (config_val % 4 == 0):
                    # Usar PM25raw en lugar de PM25
                    entry["PM25"] = entry.get("PM25raw", entry["PM25"])
            
            # Ajustar PM25 para estaciones específicas
            if station in estaciones_ajuste_pm25 and "PM25" in entry:
                # Verificar que PM25 no sea None antes de hacer el cálculo
                if entry["PM25"] is not None:
                    entry["PM25"] = ((1207 * float(entry["PM25"])) / 1000) - 1.01
                # Si es None, podemos dejarlo como None o asignarle un valor por defecto
                # Por ejemplo: entry["PM25"] = 0  # si quieres usar 0 como valor por defecto
            
            entry["station"] = station
            records.append(entry)
    df = pd.DataFrame(records)
    df['date'] = pd.to_datetime(df['date'])
    return df

def create_animation(df, output_path, fps=2, size_scale=2, map_style='osm', alpha=1.0, zoom=10, zoom_base=12,
                     aspect_ratio='1:1', center_lat=4.6257, center_lon=-74.1340):
    df = df.sort_values('date')
    grouped = df.groupby('date')
    sorted_df_dates = sorted(df['date'].unique())
    center = [center_lon, center_lat]
    
    # Calculate resolution and extent
    resolution = 360 / (2 ** zoom)
    
    # Aspect ratios configuration
    aspect_ratios = {
        '1:1': (10, 10),
        '4:3': (12, 9),
        '16:9': (16, 9),
    }
    fig_size = aspect_ratios.get(aspect_ratio, (10, 10))
    
    # Calculate extent based on aspect ratio
    aspect_ratio_value = fig_size[0] / fig_size[1]
    half_size_lon = 0.5 * resolution * aspect_ratio_value
    half_size_lat = 0.5 * resolution
    
    extent = [
        center[0] - half_size_lon, center[0] + half_size_lon,
        center[1] - half_size_lat, center[1] + half_size_lat
    ]

    # Fuentes de mapas gratuitas y sin API key
    tile_sources = {
        # Mapas base estándar
        'osm': ctx.providers.OpenStreetMap.Mapnik,  # Mapa estándar OpenStreetMap
        'osm_hot': ctx.providers.OpenStreetMap.HOT,  # Estilo humanitario de OSM
        'osm_de': ctx.providers.OpenStreetMap.DE,  # Estilo alemán de OSM
        
        # Mapas sencillos
        'cartodb_positron': ctx.providers.CartoDB.Positron,  # Estilo claro de CartoDB
        'cartodb_dark': ctx.providers.CartoDB.DarkMatter,  # Estilo oscuro de CartoDB

        # Mapa satelital
        'satellite': ctx.providers.Esri.WorldImagery,
    }

    # Obtener el proveedor de tiles seleccionado o usar OSM por defecto
    tile_source = tile_sources.get(map_style, ctx.providers.OpenStreetMap.Mapnik)

    # Ajustar la calidad de imagen según el proveedor
    provider_dpis = {
        'osm': 150,
        'osm_hot': 150,
        'osm_de': 150,
        'cartodb_positron':150,  # Mayor DPI para mapas CartoDB
        'cartodb_dark': 250,
        'satellite': 120,
    }

    base_dpi = provider_dpis.get(map_style, 120)  # DPI por defecto
    if zoom <= 5:
        base_dpi = max(50, base_dpi - 50)
    elif zoom <= 8:
        base_dpi = max(72, base_dpi - 30)
    elif zoom <= 11:
        base_dpi = max(120, base_dpi - 10)

    fig = plt.figure(figsize=fig_size, dpi=base_dpi)
    ax = plt.axes(projection=ccrs.PlateCarree())
    ax.set_extent(extent, crs=ccrs.PlateCarree())

    # Remove the problematic aspect ratio setting
    # ax.set_aspect(fig_size[1] / fig_size[0])  # Remove this line

    ctx.add_basemap(ax, source=tile_source, crs=ccrs.PlateCarree(), zoom=zoom_base, alpha=alpha)

    # Adjust subplot parameters based on aspect ratio
    if aspect_ratio == '16:9':
        plt.subplots_adjust(left=0.02, right=0.98, top=0.95, bottom=0.05)
    elif aspect_ratio == '4:3':
        plt.subplots_adjust(left=0.02, right=0.98, top=0.95, bottom=0.05)
    else:  # 1:1
        plt.subplots_adjust(left=0.02, right=0.99, top=0.95, bottom=0.02)

    # Rest of your existing code remains the same
    ax.add_feature(cfeature.BORDERS, linestyle=':', edgecolor='black')
    ax.add_feature(cfeature.COASTLINE, edgecolor='black')

    scatter = ax.scatter(
        [], [], s=10 * size_scale, transform=ccrs.PlateCarree(),
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
        current_time = sorted_df_dates[frame]
        app.logger.debug(f"Actualizando frame para la fecha: {current_time}")
        data_frame = grouped.get_group(current_time)
        data_frame = data_frame[data_frame['InOut'] == 0.0]

        # Spatial filtering based on map extent
        extent = ax.get_extent()
        data_frame = data_frame[
            (data_frame['Longitude'] >= extent[0]) &
            (data_frame['Longitude'] <= extent[1]) &
            (data_frame['Latitude'] >= extent[2]) &
            (data_frame['Latitude'] <= extent[3])
        ]

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
        size_scale = request.form.get('size_scale', 5, type=int)
        map_style = request.form.get('map_style', 'osm')
        alpha = request.form.get('alpha', 1.0, type=float)
        aspect_ratio = request.form.get('aspect_ratio', '1:1')
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
            create_animation(df, output_file, fps=fps, size_scale=size_scale, map_style=map_style, alpha=alpha,
                             zoom=zoom, zoom_base=zoom_base, aspect_ratio=aspect_ratio,
                             center_lat=center_lat, center_lon=center_lon)
            if not convert_video_to_android_compatible(output_file, compatible_output_file):
                return jsonify({"error": "Error converting video for Android compatibility"})
        except Exception as e:
            return jsonify({"error": f"Error generating animation: {e}"})

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
            <input type="number" id="size_scale" name="size_scale" value="2" min="1" max="100" required><br><br>
            
            <label for="map_style">Estilo de mapa:</label><br>
            <select id="map_style" name="map_style">
                <optgroup label="Mapas Base">
                    <option value="osm">OpenStreetMap Standard</option>
                    <option value="osm_hot">OpenStreetMap Humanitarian</option>
                    <option value="osm_de">OpenStreetMap German Style</option>
                </optgroup>
                <optgroup label="Estilos CartoDB">
                    <option value="cartodb_positron">CartoDB Positron (Claro)</option>
                    <option value="cartodb_dark">CartoDB Dark Matter</option>
                </optgroup>
                    <optgroup label="Satelital">
                    <option value="satellite">Esri Satélite</option>
                </optgroup>
            </select><br><br>
            
            <label>Aspect Ratio:</label><br>
            <select name="aspect_ratio">
                <option value="1:1">1:1</option>
                <option value="4:3">4:3</option>
                <option value="16:9">16:9</option>
            </select><br><br>

            <label>Transparency (Alpha):</label><br>
            <input type="number" name="alpha" value="1.0" step="0.1" min="0.1" max="1.0"><br><br>
            
            <label for="zoom">Nivel de zoom general (1-18):</label><br>
            <input type="number" id="zoom" name="zoom" value="10" min="1" max="18" required><br><br>
            
            <label for="zoom_base">Nivel zoom mapa base (1-18), recomendado +2 zoom general:</label><br>
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
