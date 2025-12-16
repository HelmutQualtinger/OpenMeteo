# OpenMeteo Weather Data Fetcher

This project contains Python scripts to fetch weather data from OpenMeteo, process it, and store it in a database. It also includes scripts for managing town data (e.g., Austria, Switzerland) with elevation information.

## Project Structure

- `.env.example`: Example environment variables file.
- `austria_towns_data.csv`: CSV file containing Austrian town data.
- `austria_towns_with_elevation.py`: Script to process Austrian town data with elevation.
- `austria_towns.py`: Script related to Austrian town data.
- `fetch_weather_from_openmeteo.py`: Main script to fetch weather data from OpenMeteo.
- `get_weather_from_db.py`: Script to retrieve weather data from the database.
- `import_all_towns_to_db.py`: Script to import all town data into the database.
- `import_swiss_towns_to_db.py`: Script to import Swiss town data into the database.
- `import_towns_to_db.py`: General script to import town data into the database.
- `insert_weather_data.py`: Script to insert weather data into the database.
- `pyproject.toml`: Project configuration file.
- `README.md`: This file.
- `swiss_towns_data.csv`: CSV file containing Swiss town data.
- `swiss_towns_with_elevation.py`: Script to process Swiss town data with elevation.
- `swiss_towns.py`: Script related to Swiss town data.

## Setup

1.  **Clone the repository:**
    ```bash
    git clone [repository-url]
    cd OpenMeteo
    ```

2.  **Set up environment variables:**
    Copy `.env.example` to `.env` and fill in your database connection details and any API keys if required.
    ```bash
    cp .env.example .env
    # Edit .env with your specific details
    ```

3.  **Install dependencies:**
    ```bash
    pip install -e .
    # Or install from pyproject.toml
    pip install -r requirements.txt # if you have one, otherwise poetry install
    ```

## Usage

### Fetch Weather Data

To fetch weather data from OpenMeteo:
```bash
python fetch_weather_from_openmeteo.py
```

### Import Town Data

To import town data into the database:
```bash
python import_all_towns_to_db.py
# Or specific countries:
python import_austria_towns_to_db.py
python import_swiss_towns_to_db.py
```

### Get Weather Data from DB

To retrieve weather data from the database:
```bash
python get_weather_from_db.py
```

## Contributing

Contributions are welcome! Please feel free to open issues or submit pull requests.

## License

[Specify your license here]
