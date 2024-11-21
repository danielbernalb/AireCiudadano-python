from flask import Flask, request, render_template_string, send_file
import json
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import numpy as np
from io import BytesIO
import tempfile

app = Flask(__name__)

@app.route('/getdata', methods=['GET', 'POST'])
def getdata():
    if request.method == 'POST':
        # Retrieve the file and parameters from the form
        file = request.files['file']
        fps = float(request.form['fps'])
        scale = float(request.form['scale'])
        
        # Load JSON data
        data = json.load(file)
        stations_data = data['data']
        
        # Filter data points where InOut is 0.0
        filtered_data = {}
        for station, records in stations_data.items():
            filtered_records = [record for record in records if record['InOut'] == 0.0]
            if filtered_records:
                filtered_data[station] = filtered_records
        
        # Collect all timestamps
        timestamps = set()
        for records in filtered_data.values():
            for record in records:
                timestamps.add(record['date'])
        timestamps = sorted(timestamps)
        
        # Prepare data for animation
        frames = []
        for timestamp in timestamps:
            frame = {}
            for station, records in filtered_data.items():
                for record in records:
                    if record['date'] == timestamp:
                        frame[station] = {
                            'PM25': record['PM25'],
                            'Latitude': record['Latitude'],
                            'Longitude': record['Longitude']
                        }
                        break
            frames.append(frame)
        
        # Define color mapping
        def get_color(pm25):
            if pm25 < 12:
                return 'green'
            elif 12 <= pm25 < 34:
                return 'yellow'
            elif 34 <= pm25 < 54:
                return 'orange'
            elif 54 <= pm25 < 149:
                return 'red'
            elif 149 <= pm25 < 249:
                return 'purple'
            else:
                return 'brown'
        
        # Set up the plot with cartopy
        fig = plt.figure()
        ax = fig.add_subplot(1, 1, 1, projection=ccrs.PlateCarree())
        ax.set_global()
        ax.add_feature(cfeature.COASTLINE)
        ax.add_feature(cfeature.BORDERS)
        ax.add_feature(cfeature.LAND, color='lightgray')
        ax.add_feature(cfeature.OCEAN, color='white')
        
        # Find min and max latitude and longitude for zooming
        lats = [record['Latitude'] for records in filtered_data.values() for record in records]
        lons = [record['Longitude'] for records in filtered_data.values() for record in records]
        ax.set_extent([min(lons)-0.1, max(lons)+0.1, min(lats)-0.1, max(lats)+0.1], crs=ccrs.PlateCarree())
        
        # Scatter plot placeholder
        scat = ax.scatter([], [], c=[], s=scale, transform=ccrs.PlateCarree())
        
        # Add legend
        legend_elements = [
            plt.Line2D([0], [0], marker='o', color='w', label='0 - 12', markerfacecolor='green', markersize=10),
            plt.Line2D([0], [0], marker='o', color='w', label='13 - 34', markerfacecolor='yellow', markersize=10),
            plt.Line2D([0], [0], marker='o', color='w', label='35 - 54', markerfacecolor='orange', markersize=10),
            plt.Line2D([0], [0], marker='o', color='w', label='55 - 149', markerfacecolor='red', markersize=10),
            plt.Line2D([0], [0], marker='o', color='w', label='150 - 249', markerfacecolor='purple', markersize=10),
            plt.Line2D([0], [0], marker='o', color='w', label='>=250', markerfacecolor='brown', markersize=10)
        ]
        ax.legend(handles=legend_elements, loc='lower left')
        
        # Text for date and time
        text = ax.text(min(lons)-0.05, max(lats)+0.05, '', transform=ccrs.PlateCarree(), ha='left', va='top')
        
        # Animation function
        def animate(i):
            frame_data = frames[i]
            lons = [record['Longitude'] for record in frame_data.values()]
            lats = [record['Latitude'] for record in frame_data.values()]
            pm25s = [record['PM25'] for record in frame_data.values()]
            colors = [get_color(pm25) for pm25 in pm25s]
            scat.set_offsets(np.column_stack((lons, lats)))
            scat.set_color(colors)
            scat.set_sizes([scale]*len(lons))
            text.set_text(timestamps[i])
            return scat, text
        
        # Create animation
        ani = animation.FuncAnimation(fig, animate, frames=len(frames), interval=1000/fps)
        
        # Save animation to a temporary file
        with tempfile.NamedTemporaryFile(suffix='.mp4', delete=False) as tmp_file:
            ani.save(tmp_file.name, writer=animation.FFMpegWriter(fps=fps))
            tmp_file.seek(0)
            response = send_file(
                tmp_file.name,
                as_attachment=True,
                download_name='animation.mp4',
                mimetype='video/mp4'
            )
        
        return response
    
    # HTML form embedded in Python code
    html_content = '''
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>Upload JSON and Set Parameters</title>
    </head>
    <body>
        <h1>Upload JSON File and Set Animation Parameters</h1>
        <form method="post" enctype="multipart/form-data" action="/getdata">
            <input type="file" name="file" required><br>
            <label for="fps">Animation FPS:</label>
            <input type="number" step="0.1" name="fps" value="10" required><br>
            <label for="scale">Point Size Scale:</label>
            <input type="number" step="1" name="scale" value="50" required><br>
            <button type="submit">Generate Animation</button>
        </form>
    </body>
    </html>
    '''
    return render_template_string(html_content)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8084)
