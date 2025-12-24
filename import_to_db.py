#!/usr/bin/env python3
"""
Skript zum Importieren der german_cities_50.csv in die MySQL-Datenbank
"""

import csv
import mysql.connector
from dotenv import load_dotenv
import os

# Lade Umgebungsvariablen aus .env
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
DB_TOWN_TABLE = os.getenv("DB_TOWN_TABLE")

CSV_FILE = "german_cities_50.csv"


def import_cities_to_db():
    """Importiere Städte aus CSV in die Datenbank"""
    try:
        # Verbindung zur Datenbank herstellen
        connection = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )

        cursor = connection.cursor()

        # Lese CSV-Datei
        with open(CSV_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        print(f"Importiere {len(rows)} Städte in die Tabelle '{DB_TOWN_TABLE}'...\n")

        inserted = 0
        updated = 0
        errors = 0

        for i, row in enumerate(rows, 1):
            try:
                # Überprüfe, ob die Stadt bereits in der Datenbank existiert
                check_query = f"""
                    SELECT id FROM {DB_TOWN_TABLE}
                    WHERE name = %s AND country = %s
                """
                cursor.execute(check_query, (row['name'], row['country']))
                existing = cursor.fetchone()

                if existing:
                    # Update existierende Stadt
                    update_query = f"""
                        UPDATE {DB_TOWN_TABLE}
                        SET population = %s, latitude = %s, longitude = %s,
                            elevation = %s, region = %s
                        WHERE id = %s
                    """
                    cursor.execute(update_query, (
                        int(row['population']),
                        float(row['latitude']),
                        float(row['longitude']),
                        int(row['elevation']),
                        row['region'],
                        existing[0]
                    ))
                    updated += 1
                    status = "✓ Updated"
                else:
                    # Füge neue Stadt ein
                    insert_query = f"""
                        INSERT INTO {DB_TOWN_TABLE}
                        (name, population, latitude, longitude, elevation, country, region)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_query, (
                        row['name'],
                        int(row['population']),
                        float(row['latitude']),
                        float(row['longitude']),
                        int(row['elevation']),
                        row['country'],
                        row['region']
                    ))
                    inserted += 1
                    status = "✓ Inserted"

                print(f"[{i:2d}/{len(rows)}] {row['name']:25s} {status}")

            except Exception as e:
                print(f"[{i:2d}/{len(rows)}] {row['name']:25s} ✗ Fehler: {e}")
                errors += 1

        # Commit der Änderungen
        connection.commit()
        cursor.close()
        connection.close()

        print(f"\n{'='*60}")
        print(f"Importierung abgeschlossen:")
        print(f"  • Neue Einträge: {inserted}")
        print(f"  • Aktualisierte Einträge: {updated}")
        print(f"  • Fehler: {errors}")
        print(f"  • Gesamt: {inserted + updated + errors}")
        print(f"{'='*60}")

    except mysql.connector.Error as err:
        print(f"✗ Datenbankfehler: {err}")
    except FileNotFoundError:
        print(f"✗ Datei '{CSV_FILE}' nicht gefunden!")
    except Exception as e:
        print(f"✗ Fehler: {e}")


if __name__ == "__main__":
    import_cities_to_db()
