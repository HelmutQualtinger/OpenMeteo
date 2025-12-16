import pandas as pd
from sqlalchemy import create_engine, text
import os
import sys
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- Configuration ---
CSV_FILE = 'all_towns_data.csv'
DATABASE_TABLE = os.getenv('DB_TOWN_TABLE', 'towns')

# MySQL Connection Details from environment variables
DB_HOST = os.getenv('DB_HOST')
DB_PORT = int(os.getenv('DB_PORT', 3306))
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')

# --- Main upload logic ---
def upload_csv_to_mysql(csv_file: str, table_name: str):
    """Uploads data from a CSV file to a MySQL database table."""
    try:
        df = pd.read_csv(csv_file)
        print(f"Successfully read {len(df)} rows from {csv_file}")

        # Construct MySQL connection URI
        # Using mysql+mysqlconnector for SQLAlchemy
        DATABASE_URI = f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        
        engine = create_engine(DATABASE_URI)
        
        print(f"Connecting to MySQL database: {DB_NAME} on {DB_HOST}:{DB_PORT}")

        # Upload DataFrame to MySQL
        # if_exists='replace' will drop the table and recreate it, then insert data.
        # index=False prevents pandas from writing the DataFrame index as a column in the SQL table.
        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)

        # Add auto-increment primary key to the table
        with engine.connect() as connection:
            connection.execute(text(f"""
                ALTER TABLE {table_name}
                ADD COLUMN id INT AUTO_INCREMENT UNIQUE PRIMARY KEY FIRST
            """))
            connection.commit()

        print(f"Successfully uploaded data from '{csv_file}' to '{DB_NAME}' table '{table_name}'.")
        return True

    except FileNotFoundError:
        print(f"Error: The file '{csv_file}' was not found.")
        return False
    except Exception as e:
        print(f"An error occurred during database upload: {e}")
        return False

if __name__ == "__main__":
    # Ensure the CSV file exists before attempting to upload
    if not os.path.exists(CSV_FILE):
        print(f"Error: The CSV file '{CSV_FILE}' does not exist in the current directory.")
        sys.exit(1)

    if upload_csv_to_mysql(CSV_FILE, DATABASE_TABLE):
        print(f"Data upload process completed successfully.")
    else:
        print(f"Data upload process failed.")
        sys.exit(1)