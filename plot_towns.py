import mysql.connector
import json
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Database credentials from .env
DB_HOST = os.getenv('DB_HOST')
DB_PORT = int(os.getenv('DB_PORT', 3306))
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')
DB_TOWN_TABLE = os.getenv('DB_TOWN_TABLE', 'towns')

# Connect to database
conn = mysql.connector.connect(
    host=DB_HOST,
    port=DB_PORT,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_NAME
)

cursor = conn.cursor(dictionary=True)
query = f"SELECT name, latitude, longitude, country FROM {DB_TOWN_TABLE}"
cursor.execute(query)
towns = cursor.fetchall()
conn.close()

print(f"Loaded {len(towns)} towns from database")

# Color mapping by country
colors = {'AT': '#ED2939', 'CH': '#FF0000', 'IT': '#009246'}

# Create Plotly HTML manually
towns_json = json.dumps(towns)
html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        h1 {{ color: #333; }}
        #map {{ width: 100%; height: 800px; }}
        .legend {{ margin-top: 20px; }}
        .legend-item {{ margin: 5px 0; }}
        .color-box {{ display: inline-block; width: 20px; height: 20px; margin-right: 10px; vertical-align: middle; }}
    </style>
</head>
<body>
    <h1>Towns in Europe by Country</h1>
    <div id="map"></div>
    <div class="legend">
        <h3>Countries</h3>
        <div class="legend-item"><div class="color-box" style="background-color: #ED2939;"></div>Austria (AT)</div>
        <div class="legend-item"><div class="color-box" style="background-color: #FF0000;"></div>Switzerland (CH)</div>
        <div class="legend-item"><div class="color-box" style="background-color: #009246;"></div>Italy (IT)</div>
    </div>
    <script>
        var towns = {towns_json};

        // Separate towns by country
        var at_towns = towns.filter(t => t.country === 'AT');
        var ch_towns = towns.filter(t => t.country === 'CH');
        var it_towns = towns.filter(t => t.country === 'IT');

        // Create traces for each country
        var traces = [
            {{
                x: at_towns.map(t => t.longitude),
                y: at_towns.map(t => t.latitude),
                mode: 'markers',
                type: 'scattergeo',
                name: 'Austria (AT)',
                text: at_towns.map(t => t.name + '<br>Lat: ' + t.latitude.toFixed(4) + '<br>Lon: ' + t.longitude.toFixed(4)),
                hovertemplate: '{{text}}<extra></extra>',
                marker: {{
                    size: 6,
                    color: '#ED2939',
                    opacity: 0.8
                }}
            }},
            {{
                x: ch_towns.map(t => t.longitude),
                y: ch_towns.map(t => t.latitude),
                mode: 'markers',
                type: 'scattergeo',
                name: 'Switzerland (CH)',
                text: ch_towns.map(t => t.name + '<br>Lat: ' + t.latitude.toFixed(4) + '<br>Lon: ' + t.longitude.toFixed(4)),
                hovertemplate: '{{text}}<extra></extra>',
                marker: {{
                    size: 6,
                    color: '#FF0000',
                    opacity: 0.8
                }}
            }},
            {{
                x: it_towns.map(t => t.longitude),
                y: it_towns.map(t => t.latitude),
                mode: 'markers',
                type: 'scattergeo',
                name: 'Italy (IT)',
                text: it_towns.map(t => t.name + '<br>Lat: ' + t.latitude.toFixed(4) + '<br>Lon: ' + t.longitude.toFixed(4)),
                hovertemplate: '{{text}}<extra></extra>',
                marker: {{
                    size: 6,
                    color: '#009246',
                    opacity: 0.8
                }}
            }}
        ];

        var layout = {{
            title: 'Towns in Europe by Country',
            geo: {{
                scope: 'europe',
                showland: true,
                landcolor: 'rgb(243, 243, 243)',
                coastcolor: 'rgb(204, 204, 204)',
                projection: {{type: 'natural earth'}},
                lataxis: {{range: [43, 49]}},
                lonaxis: {{range: [6, 17]}}
            }},
            hovermode: 'closest',
            margin: {{l: 0, r: 0, t: 50, b: 0}}
        }};

        Plotly.newPlot('map', traces, layout, {{responsive: true}});
    </script>
</body>
</html>
"""

output_file = 'towns_plot.html'
with open(output_file, 'w') as f:
    f.write(html_content)

print(f"Plot saved to {output_file}")

# Print summary
country_counts = {}
for town in towns:
    country = town['country']
    country_counts[country] = country_counts.get(country, 0) + 1

print("\nTown count by country:")
for country in sorted(country_counts.keys()):
    print(f"  {country}: {country_counts[country]}")
