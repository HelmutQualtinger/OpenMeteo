#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generates an HTML gallery page from a list of town names,
using placeholder images for each town.
"""

import os
from urllib.parse import quote_plus

TOWNS_FILE = 'towns.names'
OUTPUT_FILE = 'towns_gallery.html'

def read_town_names(filename):
    """Reads a list of town names from a file."""
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            return [line.strip() for line in f if line.strip()]
    except IOError as e:
        print(f"Error reading file {filename}: {e}")
        return []

def generate_html_gallery(town_names):
    """Generates HTML content for a gallery of towns."""
    html_content = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Towns Gallery</title>
    <style>
        body {
            font-family: sans-serif;
            background-color: #f0f0f0;
            margin: 0;
            padding: 20px;
        }
        h1 {
            text-align: center;
            color: #333;
        }
        .gallery {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
            gap: 20px;
        }
        .town-card {
            background-color: #fff;
            border: 1px solid #ddd;
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
            transition: transform 0.2s;
        }
        .town-card:hover {
            transform: scale(1.05);
        }
        .town-card img {
            width: 100%;
            height: 200px;
            object-fit: cover;
        }
        .town-name {
            padding: 15px;
            font-weight: bold;
            text-align: center;
        }
    </style>
</head>
<body>
    <h1>Towns Gallery</h1>
    <div class="gallery">
"""

    for town in town_names:
        encoded_town = quote_plus(town)
        placeholder_url = f"https://placehold.co/600x400/EEE/31343C?text={encoded_town}"
        html_content += f"""
        <div class="town-card">
            <img src="{placeholder_url}" alt="Photo of {town}">
            <div class="town-name">{town}</div>
        </div>
"""

    html_content += """
    </div>
</body>
</html>
"""
    return html_content

def write_html_file(html_content, filename):
    """Writes HTML content to a file."""
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(html_content)
        print(f"Successfully generated gallery at {filename}")
    except IOError as e:
        print(f"Error writing to file {filename}: {e}")

def main():
    """Main function."""
    print(f"Reading towns from {TOWNS_FILE}...")
    town_names = read_town_names(TOWNS_FILE)
    if town_names:
        print(f"Generating HTML gallery for {len(town_names)} towns...")
        html_content = generate_html_gallery(town_names)
        write_html_file(html_content, OUTPUT_FILE)
    else:
        print("No town names to process.")

if __name__ == '__main__':
    main()
