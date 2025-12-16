import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# MySQL Connection Details from environment variables
DB_HOST = os.getenv('DB_HOST')
DB_PORT = int(os.getenv('DB_PORT', 3306))
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')
TOWNS_TABLE = os.getenv('DB_TOWN_TABLE', 'towns')
WEATHER_TABLE = os.getenv('DB_WEATHER_TABLE', 'weather_data')

# Construct MySQL connection URI
DATABASE_URI = f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def create_towns_weather_view():
    """Creates a view joining towns and weather_data tables."""
    try:
        engine = create_engine(DATABASE_URI)

        print(f"Connecting to MySQL database: {DB_NAME} on {DB_HOST}:{DB_PORT}")

        # SQL query to create the view
        # Explicitly select all columns to avoid duplicate 'id' column names
        create_view_query = f"""
            CREATE OR REPLACE VIEW towns_weather_view AS
            SELECT
                t.id as town_id,
                t.name,
                t.population,
                t.latitude,
                t.longitude,
                t.elevation,
                t.country,
                t.region,
                w.id as weather_id,
                w.town_id as weather_town_id,
                w.timestamp,
                w.temperature,
                w.relative_humidity,
                w.apparent_temperature,
                w.weather_code,
                w.wind_speed,
                w.wind_direction,
                w.wind_gusts,
                w.pressure_msl,
                w.cloud_cover,
                w.uv_index,
                w.is_day,
                w.precipitation,
                w.precipitation_probability,
                w.dew_point,
                w.visibility,
                w.soil_temperature_0cm,
                w.soil_moisture_0_1cm,
                w.shortwave_radiation,
                w.direct_radiation,
                w.diffuse_radiation,
                w.direct_normal_irradiance,
                w.description,
                w.weather_main,
                w.created_at,
                w.updated_at
            FROM {TOWNS_TABLE} t
            INNER JOIN {WEATHER_TABLE} w ON t.id = w.town_id
        """

        # Execute the CREATE VIEW statement
        with engine.connect() as connection:
            connection.execute(text(create_view_query))
            connection.commit()

        print(f"✓ Successfully created view 'towns_weather_view'")
        print(f"  View joins {TOWNS_TABLE} and {WEATHER_TABLE} tables on t.id = w.town_id")
        print(f"  View name: towns_weather_view")

        # Query the view to verify and show structure
        import pandas as pd
        verify_df = pd.read_sql(text("SELECT * FROM towns_weather_view LIMIT 5"), engine)

        print(f"\n  View columns ({len(verify_df.columns)}): {list(verify_df.columns)}")
        print(f"\n  First 5 rows from view:")
        print(verify_df)

        return True

    except Exception as e:
        print(f"✗ An error occurred while creating the view: {e}")
        return False

if __name__ == "__main__":
    success = create_towns_weather_view()

    if success:
        print("\n✓ View creation process completed successfully")
    else:
        print("\n✗ View creation process failed")