#!/usr/bin/env python3
"""
Abrufen der 50 bevölkerungsreichsten deutschen Städte
mit Koordinaten, Seehöhe und Bundesland.
"""

import csv
import time
import requests
from typing import Dict, Optional

# Die bevölkerungsreichsten deutschen Städte (Quelle: Statistisches Bundesamt, Wikipedia)
GERMAN_CITIES = [
    {"name": "Berlin", "population": 3645000, "region": "Berlin"},
    {"name": "Hamburg", "population": 1844000, "region": "Hamburg"},
    {"name": "München", "population": 1471508, "region": "Bayern"},
    {"name": "Köln", "population": 1085664, "region": "Nordrhein-Westfalen"},
    {"name": "Frankfurt am Main", "population": 750056, "region": "Hessen"},
    {"name": "Stuttgart", "population": 623738, "region": "Baden-Württemberg"},
    {"name": "Düsseldorf", "population": 621877, "region": "Nordrhein-Westfalen"},
    {"name": "Dortmund", "population": 587181, "region": "Nordrhein-Westfalen"},
    {"name": "Essen", "population": 582760, "region": "Nordrhein-Westfalen"},
    {"name": "Leipzig", "population": 628717, "region": "Sachsen"},
    {"name": "Hannover", "population": 534830, "region": "Niedersachsen"},
    {"name": "Nürnberg", "population": 518365, "region": "Bayern"},
    {"name": "Dresden", "population": 556871, "region": "Sachsen"},
    {"name": "Duisburg", "population": 495784, "region": "Nordrhein-Westfalen"},
    {"name": "Bochum", "population": 364760, "region": "Nordrhein-Westfalen"},
    {"name": "Wuppertal", "population": 354262, "region": "Nordrhein-Westfalen"},
    {"name": "Bielefeld", "population": 333846, "region": "Nordrhein-Westfalen"},
    {"name": "Bonn", "population": 330779, "region": "Nordrhein-Westfalen"},
    {"name": "Mannheim", "population": 307621, "region": "Baden-Württemberg"},
    {"name": "Karlsruhe", "population": 308436, "region": "Baden-Württemberg"},
    {"name": "Augsburg", "population": 304413, "region": "Bayern"},
    {"name": "Wiesbaden", "population": 278297, "region": "Hessen"},
    {"name": "Mönchengladbach", "population": 256135, "region": "Nordrhein-Westfalen"},
    {"name": "Gelsenkirchen", "population": 255428, "region": "Nordrhein-Westfalen"},
    {"name": "Braunschweig", "population": 250556, "region": "Niedersachsen"},
    {"name": "Chemnitz", "population": 247421, "region": "Sachsen"},
    {"name": "Kiel", "population": 246714, "region": "Schleswig-Holstein"},
    {"name": "Aachen", "population": 258849, "region": "Nordrhein-Westfalen"},
    {"name": "Magdeburg", "population": 231607, "region": "Sachsen-Anhalt"},
    {"name": "Freiburg im Breisgau", "population": 230940, "region": "Baden-Württemberg"},
    {"name": "Lübeck", "population": 219307, "region": "Schleswig-Holstein"},
    {"name": "Mainz", "population": 217626, "region": "Rheinland-Pfalz"},
    {"name": "Erfurt", "population": 213171, "region": "Thüringen"},
    {"name": "Rostock", "population": 207452, "region": "Mecklenburg-Vorpommern"},
    {"name": "Saarbrücken", "population": 177874, "region": "Saarland"},
    {"name": "Halle an der Saale", "population": 236797, "region": "Sachsen-Anhalt"},
    {"name": "Heidelberg", "population": 161430, "region": "Baden-Württemberg"},
    {"name": "Potsdam", "population": 183300, "region": "Brandenburg"},
    {"name": "Pforzheim", "population": 119755, "region": "Baden-Württemberg"},
    {"name": "Wolfsburg", "population": 125746, "region": "Niedersachsen"},
    {"name": "Göttingen", "population": 117433, "region": "Niedersachsen"},
    {"name": "Offenbach am Main", "population": 129503, "region": "Hessen"},
    {"name": "Ulm", "population": 125928, "region": "Baden-Württemberg"},
    {"name": "Solingen", "population": 158913, "region": "Nordrhein-Westfalen"},
    {"name": "Leverkusen", "population": 161882, "region": "Nordrhein-Westfalen"},
    {"name": "Oldenburg", "population": 169265, "region": "Niedersachsen"},
    {"name": "Osnabrück", "population": 165053, "region": "Niedersachsen"},
    {"name": "Krefeld", "population": 222922, "region": "Nordrhein-Westfalen"},
    {"name": "Darmstadt", "population": 158351, "region": "Hessen"},
    {"name": "Schwerin", "population": 159751, "region": "Mecklenburg-Vorpommern"},
]


def get_coordinates_and_elevation(city_name: str) -> Optional[Dict]:
    """
    Abrufen von Koordinaten und Seehöhe über die Nominatim API (OpenStreetMap)
    """
    try:
        headers = {"User-Agent": "GermanCityDataCollector/1.0"}

        # Nominatim API für Koordinaten
        response = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={
                "q": f"{city_name}, Deutschland",
                "format": "json",
                "limit": 1
            },
            headers=headers,
            timeout=10
        )

        if response.status_code != 200 or not response.json():
            print(f"  ⚠ Nominatim: Keine Ergebnisse für {city_name}")
            return None

        data = response.json()[0]
        latitude = float(data["lat"])
        longitude = float(data["lon"])

        # Elevation von Open-Elevation API abrufen
        elevation = get_elevation(latitude, longitude)

        return {
            "latitude": latitude,
            "longitude": longitude,
            "elevation": elevation
        }
    except Exception as e:
        print(f"  ⚠ Fehler beim Abrufen von {city_name}: {e}")
        return None


def get_elevation(latitude: float, longitude: float) -> int:
    """
    Seehöhe über die Open-Elevation API abrufen
    """
    try:
        response = requests.get(
            "https://api.open-elevation.com/api/v1/lookup",
            params={"locations": f"{latitude},{longitude}"},
            timeout=10
        )

        if response.status_code == 200:
            data = response.json()
            if data.get("results"):
                elevation = data["results"][0].get("elevation")
                return int(elevation) if elevation is not None else 0
        return 0
    except Exception as e:
        print(f"  ⚠ Fehler beim Abrufen der Seehöhe: {e}")
        return 0


def main():
    """Hauptfunktion"""
    results = []

    print(f"Verarbeite {len(GERMAN_CITIES)} deutsche Städte...\n")

    for i, city_data in enumerate(GERMAN_CITIES, 1):
        print(f"[{i:2d}/{len(GERMAN_CITIES)}] {city_data['name']:25s}", end=" ... ", flush=True)

        # Versuchen, Koordinaten und Seehöhe zu erhalten
        location_data = get_coordinates_and_elevation(city_data["name"])

        if location_data:
            results.append({
                "name": city_data["name"],
                "population": city_data["population"],
                "latitude": round(location_data["latitude"], 4),
                "longitude": round(location_data["longitude"], 4),
                "elevation": location_data["elevation"],
                "country": "DE",
                "region": city_data["region"]
            })
            print("✓")
        else:
            print("✗")

        # Rate limiting um die API nicht zu überlasten
        time.sleep(0.5)

    # Sortiere nach Bevölkerung absteigend
    results.sort(key=lambda x: x["population"], reverse=True)

    # Speichere die ersten 50
    results = results[:50]

    # Speichere in CSV
    with open("german_cities_50.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["name", "population", "latitude", "longitude", "elevation", "country", "region"]
        )
        writer.writeheader()
        writer.writerows(results)

    print(f"\n✓ {len(results)} Städte in 'german_cities_50.csv' gespeichert")

    # Zeige die Top 10
    print("\nTop 10 bevölkerungsreichste deutsche Städte:")
    print("-" * 60)
    for i, city in enumerate(results[:10], 1):
        print(f"{i:2d}. {city['name']:25s} {city['population']:>10,} Einwohner")


if __name__ == "__main__":
    main()
