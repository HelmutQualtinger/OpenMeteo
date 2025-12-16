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

def create_search_indexes():
    """Creates indexes on towns and weather_data tables for optimized search queries."""
    try:
        engine = create_engine(DATABASE_URI)

        print(f"Connecting to MySQL database: {DB_NAME} on {DB_HOST}:{DB_PORT}")
        print(f"Creating indexes for search optimization...\n")

        indexes_to_create = [
            # Indexes on towns table for country/region/town searches
            {
                'table': TOWNS_TABLE,
                'name': f'idx_{TOWNS_TABLE}_country',
                'columns': 'country(50)',
                'description': 'Single column index on country (first 50 chars)'
            },
            {
                'table': TOWNS_TABLE,
                'name': f'idx_{TOWNS_TABLE}_country_region',
                'columns': 'country(50), region(50)',
                'description': 'Composite index on country and region'
            },
            {
                'table': TOWNS_TABLE,
                'name': f'idx_{TOWNS_TABLE}_country_region_name',
                'columns': 'country(50), region(50), name(100)',
                'description': 'Composite index on country, region, and town name'
            },
            # Indexes on weather_data table for time range searches
            {
                'table': WEATHER_TABLE,
                'name': f'idx_{WEATHER_TABLE}_timestamp',
                'columns': 'timestamp',
                'description': 'Single column index on timestamp'
            },
            {
                'table': WEATHER_TABLE,
                'name': f'idx_{WEATHER_TABLE}_town_id_timestamp',
                'columns': 'town_id, timestamp',
                'description': 'Composite index on town_id and timestamp for join + time filter'
            },
        ]

        with engine.connect() as connection:
            for idx in indexes_to_create:
                try:
                    create_index_query = f"""
                        CREATE INDEX {idx['name']}
                        ON {idx['table']} ({idx['columns']})
                    """
                    connection.execute(text(create_index_query))
                    connection.commit()
                    print(f"✓ Created: {idx['name']}")
                    print(f"  Table: {idx['table']}")
                    print(f"  Columns: {idx['columns']}")
                    print(f"  Purpose: {idx['description']}\n")

                except Exception as e:
                    if 'Duplicate key name' in str(e):
                        print(f"⚠ Index already exists: {idx['name']}\n")
                    else:
                        print(f"✗ Error creating {idx['name']}: {e}\n")

        # Show created indexes
        print("\n" + "="*70)
        print("INDEXES SUMMARY")
        print("="*70)

        with engine.connect() as connection:
            for table in [TOWNS_TABLE, WEATHER_TABLE]:
                show_indexes_query = f"SHOW INDEX FROM {table}"
                result = connection.execute(text(show_indexes_query))
                rows = result.fetchall()

                print(f"\nIndexes on {table}:")
                for row in rows:
                    print(f"  - {row[2]}: {row[4]} (Column: {row[5]})")

        print("\n✓ Index creation process completed successfully")
        return True

    except Exception as e:
        print(f"✗ An error occurred: {e}")
        return False

if __name__ == "__main__":
    create_search_indexes()