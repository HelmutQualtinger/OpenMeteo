import pandas as pd
from sqlalchemy import create_engine, text
import os
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

def join_towns_and_weather():
    """Joins towns and weather_data tables on town_id."""
    try:
        engine = create_engine(DATABASE_URI)

        print(f"Connecting to MySQL database: {DB_NAME} on {DB_HOST}:{DB_PORT}")

        # SQL query to join the tables
        query = f"""
            SELECT t.*, w.*
            FROM {TOWNS_TABLE} t
            INNER JOIN {WEATHER_TABLE} w ON t.id = w.town_id
        """

        # Read the joined data into a DataFrame
        df = pd.read_sql(text(query), engine)

        print(f"Successfully joined {TOWNS_TABLE} and {WEATHER_TABLE} tables")
        print(f"Total rows: {len(df)}")
        print(f"\nColumns: {list(df.columns)}")
        print(f"\nFirst few rows:")
        print(df.head())

        return df

    except Exception as e:
        print(f"An error occurred during the join operation: {e}")
        return None

if __name__ == "__main__":
    result_df = join_towns_and_weather()

    if result_df is not None:
        print("\n✓ Join operation completed successfully")
    else:
        print("\n✗ Join operation failed")