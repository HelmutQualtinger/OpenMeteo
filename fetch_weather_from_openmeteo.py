#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fetch real weather data from Open-Meteo API and store in database.
Uses coordinates from towns table to get current weather.
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv
import pymysql
from pymysql import Error
from datetime import datetime
import requests
import time

# Load environment variables from .env file
env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path)

# Database configuration from environment variables
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME')
}

# Table names from environment
TOWN_TABLE = os.getenv('DB_TOWN_TABLE', 'towns')
WEATHER_TABLE = os.getenv('DB_WEATHER_TABLE', 'weather')

# Open-Meteo API URL
OPENMETEO_API_URL = "https://api.open-meteo.com/v1/forecast"

def create_connection():
    """Create a database connection using PyMySQL."""
    try:
        connection = pymysql.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            database=DB_CONFIG['database'],
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        print("Successfully connected to MySQL Server")
        return connection
    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
        sys.exit(1)

def drop_weather_table(connection):
    """Drop weather table if it exists (for schema updates)."""
    cursor = connection.cursor()
    try:
        cursor.execute(f"DROP TABLE IF EXISTS `{WEATHER_TABLE}`")
        connection.commit()
        print(f"✅ Dropped existing table '{WEATHER_TABLE}'.")
    except Error as e:
        print(f"Error dropping table: {e}")
    finally:
        cursor.close()

def create_weather_table(connection):
    """Create weather table with all available OpenMeteo parameters."""
    cursor = connection.cursor()

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS `{WEATHER_TABLE}` (
        id INT AUTO_INCREMENT PRIMARY KEY,
        town_id INT NOT NULL,
        timestamp DATETIME NOT NULL,
        temperature DECIMAL(5, 2),
        relative_humidity INT,
        apparent_temperature DECIMAL(5, 2),
        weather_code INT,
        wind_speed DECIMAL(5, 2),
        wind_direction INT,
        wind_gusts DECIMAL(5, 2),
        pressure_msl INT,
        cloud_cover INT,
        uv_index DECIMAL(4, 2),
        is_day INT,
        precipitation DECIMAL(5, 2),
        precipitation_probability INT,
        dew_point DECIMAL(5, 2),
        visibility INT,
        soil_temperature_0cm DECIMAL(5, 2),
        soil_moisture_0_1cm DECIMAL(5, 2),
        shortwave_radiation DECIMAL(8, 2),
        direct_radiation DECIMAL(8, 2),
        diffuse_radiation DECIMAL(8, 2),
        direct_normal_irradiance DECIMAL(8, 2),
        description VARCHAR(255),
        weather_main VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY unique_town_timestamp (town_id, timestamp),
        INDEX idx_town_id (town_id),
        INDEX idx_timestamp (timestamp)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """

    try:
        cursor.execute(create_table_query)
        connection.commit()
        print(f"✅ Table '{WEATHER_TABLE}' created or already exists.")
    except Error as e:
        print(f"⚠️  Error creating table: {e}")
        print(f"Attempting to recreate table...")
        try:
            drop_weather_table(connection)
            cursor.execute(create_table_query)
            connection.commit()
            print(f"✅ Table '{WEATHER_TABLE}' successfully recreated.")
        except Error as e2:
            print(f"❌ Error recreating table: {e2}")
    finally:
        cursor.close()

def get_all_towns(connection):
    """Get all towns from the database."""
    cursor = connection.cursor()
    try:
        cursor.execute(f"SELECT id, latitude, longitude, name FROM `{TOWN_TABLE}`")
        towns = cursor.fetchall()
        return towns
    except Error as e:
        print(f"Error getting towns: {e}")
        return []
    finally:
        cursor.close()

def fetch_weather_batch(towns_data):
    """
    Fetch ALL available weather data for multiple coordinates in a single batch request.
    Open-Meteo returns an array of objects, one per location.
    Returns list of weather data dictionaries with all available parameters.
    """
    try:
        # Extract coordinates from towns data
        latitudes = [str(town['latitude']) for town in towns_data]
        longitudes = [str(town['longitude']) for town in towns_data]

        # Request all available current weather parameters from Open-Meteo
        params = {
            'latitude': ','.join(latitudes),
            'longitude': ','.join(longitudes),
            'current': 'temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,weather_code,wind_speed_10m,wind_direction_10m,wind_gusts_10m,pressure_msl,cloud_cover,uv_index,is_day,precipitation_probability,dew_point_2m,visibility,soil_temperature_0cm,soil_moisture_0_1cm,shortwave_radiation,direct_radiation,diffuse_radiation,direct_normal_irradiance',
            'timezone': 'auto'
        }

        response = requests.get(OPENMETEO_API_URL, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()
        results = []

        # When passing multiple coordinates, Open-Meteo returns an array directly
        if isinstance(data, list):
            for location in data:
                current = location.get('current', {})
                results.append({
                    'temperature': current.get('temperature_2m'),
                    'relative_humidity': current.get('relative_humidity_2m'),
                    'apparent_temperature': current.get('apparent_temperature'),
                    'weather_code': current.get('weather_code'),
                    'wind_speed': current.get('wind_speed_10m'),
                    'wind_direction': current.get('wind_direction_10m'),
                    'wind_gusts': current.get('wind_gusts_10m'),
                    'pressure_msl': current.get('pressure_msl'),
                    'cloud_cover': current.get('cloud_cover'),
                    'uv_index': current.get('uv_index'),
                    'is_day': current.get('is_day'),
                    'precipitation': current.get('precipitation'),
                    'precipitation_probability': current.get('precipitation_probability'),
                    'dew_point': current.get('dew_point_2m'),
                    'visibility': current.get('visibility'),
                    'soil_temperature_0cm': current.get('soil_temperature_0cm'),
                    'soil_moisture_0_1cm': current.get('soil_moisture_0_1cm'),
                    'shortwave_radiation': current.get('shortwave_radiation'),
                    'direct_radiation': current.get('direct_radiation'),
                    'diffuse_radiation': current.get('diffuse_radiation'),
                    'direct_normal_irradiance': current.get('direct_normal_irradiance'),
                    'timestamp': datetime.now()
                })

        return results if results else None

    except requests.RequestException as e:
        print(f"Error fetching weather from Open-Meteo: {e}")
        return None
    except (KeyError, ValueError) as e:
        print(f"Error parsing weather data: {e}")
        return None

def weather_code_to_description(code, is_day=True):
    """
    Convert WMO weather code to German description.
    Based on WMO Weather interpretation codes.
    """
    wmo_codes = {
        0: ("Klarer Himmel", "Klar"),
        1: ("Überwiegend klar", "Klar"),
        2: ("Teilweise bewölkt", "Bewölkt"),
        3: ("Bedeckt", "Bewölkt"),
        45: ("Nebelig", "Nebel"),
        48: ("Rime-Nebel", "Nebel"),
        51: ("Leichter Niesel", "Niesel"),
        53: ("Mäßiger Niesel", "Niesel"),
        55: ("Dichter Niesel", "Niesel"),
        61: ("Schwacher Regen", "Regen"),
        63: ("Mäßiger Regen", "Regen"),
        65: ("Starker Regen", "Regen"),
        71: ("Schwacher Schneefall", "Schnee"),
        73: ("Mäßiger Schneefall", "Schnee"),
        75: ("Starker Schneefall", "Schnee"),
        77: ("Schneekörner", "Schnee"),
        80: ("Schwache Regenschauer", "Regen"),
        81: ("Mäßige Regenschauer", "Regen"),
        82: ("Heftige Regenschauer", "Regen"),
        85: ("Schwache Schneeschauer", "Schnee"),
        86: ("Starke Schneeschauer", "Schnee"),
        95: ("Gewitter", "Gewitter"),
        96: ("Gewitter mit leichtem Hagel", "Gewitter"),
        99: ("Gewitter mit schweren Hagel", "Gewitter"),
    }

    description, main = wmo_codes.get(code, ("Unbekannt", "Unbekannt"))
    return description, main

def insert_or_update_weather(connection, town_id, weather_data):
    """Insert or update weather data for a town."""
    cursor = connection.cursor()

    if not weather_data:
        return False

    description, weather_main = weather_code_to_description(
        weather_data.get('weather_code', 0),
        weather_data.get('is_day', True)
    )

    insert_query = f"""
    INSERT INTO `{WEATHER_TABLE}`
    (town_id, timestamp, temperature, feels_like, humidity, wind_speed,
     wind_deg, wind_gust, pressure, clouds, uvi, description, weather_main)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        temperature = VALUES(temperature),
        feels_like = VALUES(feels_like),
        humidity = VALUES(humidity),
        wind_speed = VALUES(wind_speed),
        wind_deg = VALUES(wind_deg),
        wind_gust = VALUES(wind_gust),
        pressure = VALUES(pressure),
        clouds = VALUES(clouds),
        uvi = VALUES(uvi),
        description = VALUES(description),
        weather_main = VALUES(weather_main),
        updated_at = CURRENT_TIMESTAMP
    """

    try:
        cursor.execute(insert_query, (
            town_id,
            weather_data['timestamp'],
            weather_data.get('temperature'),
            weather_data.get('feels_like'),
            weather_data.get('humidity'),
            weather_data.get('wind_speed'),
            weather_data.get('wind_deg'),
            weather_data.get('wind_gust'),
            weather_data.get('pressure'),
            weather_data.get('clouds'),
            weather_data.get('uvi'),
            description,
            weather_main
        ))
        connection.commit()
        return True
    except Error as e:
        print(f"Error inserting weather for town {town_id}: {e}")
        return False
    finally:
        cursor.close()

def insert_all_weather(connection, towns, weather_data_list):
    """Bulk insert all weather records with all available parameters."""
    cursor = connection.cursor()

    try:
        # Prepare all data for batch insert
        values_list = []
        params = []

        for i, town in enumerate(towns):
            if i < len(weather_data_list) and weather_data_list[i]:
                weather_data = weather_data_list[i]

                description, weather_main = weather_code_to_description(
                    weather_data.get('weather_code', 0),
                    weather_data.get('is_day', True)
                )

                values_list.append("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
                params.extend([
                    town['id'],
                    weather_data['timestamp'],
                    weather_data.get('temperature'),
                    weather_data.get('relative_humidity'),
                    weather_data.get('apparent_temperature'),
                    weather_data.get('weather_code'),
                    weather_data.get('wind_speed'),
                    weather_data.get('wind_direction'),
                    weather_data.get('wind_gusts'),
                    weather_data.get('pressure_msl'),
                    weather_data.get('cloud_cover'),
                    weather_data.get('uv_index'),
                    weather_data.get('is_day'),
                    weather_data.get('precipitation'),
                    weather_data.get('precipitation_probability'),
                    weather_data.get('dew_point'),
                    weather_data.get('visibility'),
                    weather_data.get('soil_temperature_0cm'),
                    weather_data.get('soil_moisture_0_1cm'),
                    weather_data.get('shortwave_radiation'),
                    weather_data.get('direct_radiation'),
                    weather_data.get('diffuse_radiation'),
                    weather_data.get('direct_normal_irradiance'),
                    description,
                    weather_main
                ])

        # Build single INSERT with all VALUES
        if values_list:
            insert_query = f"""
            INSERT INTO `{WEATHER_TABLE}`
            (town_id, timestamp, temperature, relative_humidity, apparent_temperature, weather_code,
             wind_speed, wind_direction, wind_gusts, pressure_msl, cloud_cover, uv_index, is_day,
             precipitation, precipitation_probability, dew_point, visibility, soil_temperature_0cm,
             soil_moisture_0_1cm, shortwave_radiation, direct_radiation, diffuse_radiation,
             direct_normal_irradiance, description, weather_main)
            VALUES {','.join(values_list)}
            ON DUPLICATE KEY UPDATE
                temperature = VALUES(temperature),
                relative_humidity = VALUES(relative_humidity),
                apparent_temperature = VALUES(apparent_temperature),
                weather_code = VALUES(weather_code),
                wind_speed = VALUES(wind_speed),
                wind_direction = VALUES(wind_direction),
                wind_gusts = VALUES(wind_gusts),
                pressure_msl = VALUES(pressure_msl),
                cloud_cover = VALUES(cloud_cover),
                uv_index = VALUES(uv_index),
                is_day = VALUES(is_day),
                precipitation = VALUES(precipitation),
                precipitation_probability = VALUES(precipitation_probability),
                dew_point = VALUES(dew_point),
                visibility = VALUES(visibility),
                soil_temperature_0cm = VALUES(soil_temperature_0cm),
                soil_moisture_0_1cm = VALUES(soil_moisture_0_1cm),
                shortwave_radiation = VALUES(shortwave_radiation),
                direct_radiation = VALUES(direct_radiation),
                diffuse_radiation = VALUES(diffuse_radiation),
                direct_normal_irradiance = VALUES(direct_normal_irradiance),
                description = VALUES(description),
                weather_main = VALUES(weather_main),
                updated_at = CURRENT_TIMESTAMP
            """

            cursor.execute(insert_query, params)
            connection.commit()
            return len(values_list)
        else:
            return 0

    except Error as e:
        print(f"Error inserting bulk weather data: {e}")
        print(f"Attempting to recreate table and retry...")
        try:
            drop_weather_table(connection)
            create_weather_table(connection)
            # Retry insert after table recreation
            cursor = connection.cursor()
            values_list = []
            params = []

            for i, town in enumerate(towns):
                if i < len(weather_data_list) and weather_data_list[i]:
                    weather_data = weather_data_list[i]
                    description, weather_main = weather_code_to_description(
                        weather_data.get('weather_code', 0),
                        weather_data.get('is_day', True)
                    )
                    values_list.append("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
                    params.extend([
                        town['id'], weather_data['timestamp'],
                        weather_data.get('temperature'), weather_data.get('relative_humidity'),
                        weather_data.get('apparent_temperature'), weather_data.get('weather_code'),
                        weather_data.get('wind_speed'), weather_data.get('wind_direction'),
                        weather_data.get('wind_gusts'), weather_data.get('pressure_msl'),
                        weather_data.get('cloud_cover'), weather_data.get('uv_index'),
                        weather_data.get('is_day'), weather_data.get('precipitation'),
                        weather_data.get('precipitation_probability'), weather_data.get('dew_point'),
                        weather_data.get('visibility'), weather_data.get('soil_temperature_0cm'),
                        weather_data.get('soil_moisture_0_1cm'), weather_data.get('shortwave_radiation'),
                        weather_data.get('direct_radiation'), weather_data.get('diffuse_radiation'),
                        weather_data.get('direct_normal_irradiance'), description, weather_main
                    ])

            if values_list:
                insert_query = f"""
                INSERT INTO `{WEATHER_TABLE}`
                (town_id, timestamp, temperature, relative_humidity, apparent_temperature, weather_code,
                 wind_speed, wind_direction, wind_gusts, pressure_msl, cloud_cover, uv_index, is_day,
                 precipitation, precipitation_probability, dew_point, visibility, soil_temperature_0cm,
                 soil_moisture_0_1cm, shortwave_radiation, direct_radiation, diffuse_radiation,
                 direct_normal_irradiance, description, weather_main)
                VALUES {','.join(values_list)}
                """
                cursor.execute(insert_query, params)
                connection.commit()
                print(f"✅ Successfully inserted data after table recreation")
                return len(values_list)
        except Error as e2:
            print(f"❌ Failed to recover: {e2}")
            connection.rollback()

        return 0
    finally:
        cursor.close()

def main():
    """Main function."""
    print("Starting real weather data fetch from Open-Meteo API...\n")

    # Create connection
    connection = create_connection()

    if connection:
        try:
            # Create weather table
            create_weather_table(connection)

            # Get all towns
            towns = get_all_towns(connection)
            print(f"Found {len(towns)} towns in database.")

            if not towns:
                print("Error: No towns found.")
                sys.exit(1)

            # Fetch weather in optimized batches (URL has length limit, so use batch size of 50)
            print(f"\nFetching weather data for {len(towns)} towns from Open-Meteo (batch requests)...")

            batch_size = 10
            all_weather = []
            all_towns = []
            batch_num = 0
            total_batches = (len(towns) + batch_size - 1) // batch_size

            for batch_start in range(0, len(towns), batch_size):
                batch_num += 1
                batch_end = min(batch_start + batch_size, len(towns))
                towns_batch = towns[batch_start:batch_end]

                print(f"  Fetching batch {batch_num}/{total_batches} ({batch_end}/{len(towns)} towns)...")
                weather_batch = fetch_weather_batch(towns_batch)

                if weather_batch:
                    all_weather.extend(weather_batch)
                    all_towns.extend(towns_batch)

                # Delay between batches to respect API rate limits
                if batch_num < total_batches:
                    print(f"    Waiting 2 seconds before next batch...")
                    time.sleep(5)

            if all_weather and all_towns:
                print(f"✅ Fetched weather data for {len(all_weather)} locations.")
                print(f"Writing {len(all_weather)} weather records to database...")

                success_count = insert_all_weather(connection, all_towns, all_weather)
                error_count = len(all_towns) - success_count

                print(f"\n✅ Successfully inserted/updated weather for {success_count}/{len(all_towns)} towns.")
                if error_count > 0:
                    print(f"⚠️  {error_count} towns had errors.")
            else:
                print("❌ Failed to fetch weather data.")
                sys.exit(1)

            # Show sample data
            cursor = connection.cursor()
            cursor.execute(f"""
                SELECT w.id, w.town_id, t.name, w.temperature, w.relative_humidity,
                       w.apparent_temperature, w.wind_speed, w.wind_direction, w.wind_gusts,
                       w.precipitation, w.dew_point, w.visibility,
                       w.description, w.weather_main, w.timestamp
                FROM `{WEATHER_TABLE}` w
                JOIN `{TOWN_TABLE}` t ON w.town_id = t.id
                ORDER BY w.timestamp DESC
                LIMIT 5
            """)

            print("\n✅ Latest 5 weather records from database (with all available parameters):")
            for j, row in enumerate(cursor.fetchall(), 1):
                print(f"\n{j}. {row['name']}:")
                print(f"   Temperature: {row['temperature']}°C (feels like {row['apparent_temperature']}°C)")
                print(f"   Humidity: {row['relative_humidity']}%, Dew Point: {row['dew_point']}°C")
                print(f"   Wind: {row['wind_speed']} km/h from {row['wind_direction']}°, Gusts: {row['wind_gusts']} km/h")
                print(f"   Precipitation: {row['precipitation']} mm, Visibility: {row['visibility']} m")
                print(f"   Condition: {row['description']} ({row['weather_main']})")
                print(f"   Timestamp: {row['timestamp']}")

            cursor.close()

        finally:
            connection.close()
            print("\n✅ Database connection closed.")

if __name__ == '__main__':
    main()
