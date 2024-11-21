from flask import Flask, request, send_file
import json
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from io import BytesIO
import imageio
import numpy as np

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
        fig = plt.figure(figsize=(8, 6), dpi=100)
        ax = fig.add_subplot(1, 1, 1, projection=ccrs.PlateCarree())
        ax.set_global()
        ax.add_feature(cfeature.COASTLINE)
        ax.add_feature(cfeature.BORDERS)
        ax.add_feature(cfeature.LAND, color='lightgray')
        ax.add_feature(cfeature.OCEAN, color='white')
        
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
        
        # Initialize scatter plot
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
        text = ax.text(0, 0, '', transform=ccrs.PlateCarree(), ha='left', va='top')
        
        # Collect frames
        for timestamp in timestamps:
            # Prepare data for this frame
            lons = []
            lats = []
            pm25s = []
            for station, records in filtered_data.items():
                for record in records:
                    if record['date'] == timestamp:
                        lons.append(record['Longitude'])
                        lats.append(record['Latitude'])
                        pm25s.append(record['PM25'])
                        break
            # Update scatter plot
            scat.set_offsets(np.column_stack((lons, lats)))
            scat.set_color([get_color(pm25) for pm25 in pm25s])
            scat.set_sizes([scale]*len(lons))
            text.set_text(timestamp)
            
            # Save the figure to a BytesIO buffer
            img_buffer = BytesIO()
            plt.savefig(img_buffer, format='png', dpi=100)
            img_buffer.seek(0)
            
            # Read the image and append to frames
            image = imageio.imread(img_buffer)
            frames.append(image)
        
        # Create a BytesIO buffer for the output video
        video_buffer = BytesIO()
        imageio.mimsave(video_buffer, frames, format='mp4', fps=fps)
        video_buffer.seek(0)
        
        # Send the video buffer to the client for download
        return send_file(
            video_buffer,
            as_attachment=True,
            download_name='animation.mp4',
            mimetype='video/mp4'
        )
    
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
    app.run(debug=True)