import requests
import pandas as pd
import sys

# --- Configuration ---
# IMPORTANT: You need a free GeoNames username to use their API.
# Register at http://www.geonames.org/login and enter your username below.
# You can also pass it as a command-line argument.
if len(sys.argv) > 1:
    GEONAMES_USERNAME = sys.argv[1]
else:
    GEONAMES_USERNAME = "demo"  # Replace "demo" with your actual username

# API endpoint and parameters
GEONAMES_URL = "http://api.geonames.org/searchJSON"
PARAMS = {
    "country": "DE",
    "featureClass": "P",
    "style": "FULL",
    "maxRows": 100,
    "orderby": "population",
    "username": GEONAMES_USERNAME
}

output_csv_path = 'german_towns_data.csv'
requested_columns = ['name', 'population', 'latitude', 'longitude', 'elevation', 'country', 'region']
# --- End Configuration ---

def fetch_and_process_cities():
    if GEONAMES_USERNAME == "demo":
        print("Warning: Using the 'demo' username for GeoNames API, which has low daily limits.")
        print("For reliable results, please register for a free account at http://www.geonames.org/login")
        print("and pass your username as an argument: python fetch_german_cities.py YOUR_USERNAME")

    try:
        print(f"Fetching data from GeoNames API with username: {GEONAMES_USERNAME}...")
        response = requests.get(GEONAMES_URL, params=PARAMS)
        response.raise_for_status()

        data = response.json()

        if 'status' in data:
            print(f"Error from GeoNames API: {data['status']['message']}")
            print(f"Value: {data['status']['value']}")
            print("Please check your username and API request parameters.")
            return

        if 'geonames' not in data or not data['geonames']:
            print("No city data returned from GeoNames.")
            return

        print("Processing API response...")
        cities = data['geonames']
        city_list = []

        for city in cities:
            elevation = city.get('srtm3', city.get('gtopo30', pd.NA))
            if elevation in [-9999, -99999]:
                 elevation = pd.NA

            city_list.append({
                'name': city.get('name', ''),
                'population': city.get('population', 0),
                'latitude': city.get('lat', ''),
                'longitude': city.get('lng', ''),
                'elevation': elevation,
                'country': city.get('countryName', 'Germany'),
                'region': city.get('adminName1', '')
            })

        if not city_list:
            print("Could not parse any cities from the API response.")
            return

        df = pd.DataFrame(city_list)
        if df.empty:
            print("DataFrame is empty after processing.")
            return

        top_50_df = df.head(50)
        final_df = top_50_df[requested_columns]

        final_df.to_csv(output_csv_path, index=False)
        print(f"\nSuccessfully created '{output_csv_path}' with the top 50 most populous cities in Germany.")
        print(f"Total cities found and processed: {len(final_df)}")

    except requests.exceptions.RequestException as e:
        print(f"An error occurred while fetching data from the API: {e}")
    except KeyError as e:
        print(f"Error: A required key is missing in the API response: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    fetch_and_process_cities()
