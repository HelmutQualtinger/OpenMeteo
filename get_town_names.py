#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fetches all distinct town names from the database and saves them to a file.
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv
import pymysql
from pymysql import Error

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

# Table name from environment
TOWN_TABLE = os.getenv('DB_TOWN_TABLE', 'towns')
OUTPUT_FILE = 'towns.names'

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

def get_distinct_town_names(connection):
    """Get all distinct town names from the towns table."""
    cursor = connection.cursor()
    try:
        cursor.execute(f"SELECT DISTINCT name FROM `{TOWN_TABLE}` ORDER BY name")
        towns = cursor.fetchall()
        return [town['name'] for town in towns]
    except Error as e:
        print(f"Error getting town names: {e}")
        return []
    finally:
        cursor.close()

def write_town_names_to_file(town_names, filename):
    """Write a list of town names to a file."""
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            for name in town_names:
                f.write(f"{name}\n")
        print(f"Successfully wrote {len(town_names)} town names to {filename}")
    except IOError as e:
        print(f"Error writing to file {filename}: {e}")

def main():
    """Main function."""
    print("Fetching distinct town names from the database...")

    connection = create_connection()

    if connection:
        try:
            town_names = get_distinct_town_names(connection)
            if town_names:
                write_town_names_to_file(town_names, OUTPUT_FILE)
            else:
                print("No town names found.")
        finally:
            connection.close()
            print("Database connection closed.")

if __name__ == '__main__':
    main()
