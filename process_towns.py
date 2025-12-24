import pandas as pd
import sys
import os

# Define file paths and output file
austria_csv = 'austria_towns_data.csv'
swiss_csv = 'swiss_towns_data.csv'
italian_csv = 'italian_towns_data.csv'
output_csv = 'all_towns_data.csv'
population_column = 'population' # User confirmed this name

# --- Check if input files exist ---
if not os.path.exists(austria_csv):
    print(f"Error: File not found - {austria_csv}. Please ensure it is in the current directory.")
    sys.exit(1)
if not os.path.exists(swiss_csv):
    print(f"Error: File not found - {swiss_csv}. Please ensure it is in the current directory.")
    sys.exit(1)
if not os.path.exists(italian_csv):
    print(f"Error: File not found - {italian_csv}. Please ensure it is in the current directory.")
    sys.exit(1)

try:
    # Read Austria data
    df_austria = pd.read_csv(austria_csv)
    df_austria['country'] = 'AT'
    # Assuming 'federal_state' is the column for Austrian region
    if 'federal_state' not in df_austria.columns:
        print(f"Warning: 'federal_state' column not found in {austria_csv}. Austrian regions might not be mapped correctly.")
    print(f"Read {len(df_austria)} towns from {austria_csv}")

    # Read Swiss data
    df_swiss = pd.read_csv(swiss_csv)
    df_swiss['country'] = 'CH'
    # Assuming 'canton' is the column for Swiss region
    if 'canton' not in df_swiss.columns:
        print(f"Warning: 'canton' column not found in {swiss_csv}. Swiss regions might not be mapped correctly.")
    print(f"Read {len(df_swiss)} towns from {swiss_csv}")

    # Read Italian data
    df_italian = pd.read_csv(italian_csv)
    df_italian['country'] = 'IT'
    # Assuming 'region' is the column for Italian region
    if 'region' not in df_italian.columns:
        print(f"Warning: 'region' column not found in {italian_csv}. Italian regions might not be mapped correctly.")
    print(f"Read {len(df_italian)} towns from {italian_csv}")

    # Combine dataframes
    df_combined = pd.concat([df_austria, df_swiss, df_italian], ignore_index=True)
    print(f"Combined data has {len(df_combined)} towns.")

    # Create the 'region' column by combining 'federal_state', 'canton', and handling existing 'region'
    # Use combine_first to merge non-null values from federal_state, canton, or region
    region_cols_found = False
    if 'federal_state' in df_combined.columns and 'canton' in df_combined.columns:
        if 'region' in df_combined.columns:
            # Combine all three sources
            df_combined['region'] = df_combined['federal_state'].combine_first(df_combined['canton']).combine_first(df_combined['region'])
            df_combined = df_combined.drop(columns=['federal_state', 'canton'])
            region_cols_found = True
            print("Created 'region' column by combining 'federal_state', 'canton', and existing 'region', and dropped original columns.")
        else:
            # Combine federal_state and canton
            df_combined['region'] = df_combined['federal_state'].combine_first(df_combined['canton'])
            df_combined = df_combined.drop(columns=['federal_state', 'canton'])
            region_cols_found = True
            print("Created 'region' column by combining 'federal_state' and 'canton', and dropped original columns.")
    elif 'federal_state' in df_combined.columns:
        df_combined.rename(columns={'federal_state': 'region'}, inplace=True)
        print("Renamed 'federal_state' to 'region' for Austrian data.")
        region_cols_found = True
    elif 'canton' in df_combined.columns:
        df_combined.rename(columns={'canton': 'region'}, inplace=True)
        print("Renamed 'canton' to 'region' for Swiss data.")
        region_cols_found = True

    if not region_cols_found and 'region' not in df_combined.columns:
        print("Warning: No region column found for region mapping. 'region' column will not be populated.")

    # Convert population column to numeric, coercing errors to NaN
    df_combined[population_column] = pd.to_numeric(df_combined[population_column], errors='coerce')

    # Filter out rows where population is NaN after conversion (if any)
    df_filtered = df_combined.dropna(subset=[population_column])

    # Filter by population >= 5000
    min_population = 5000
    df_filtered = df_filtered[df_filtered[population_column] >= min_population]

    print(f"Filtered data: {len(df_filtered)} towns with '{population_column}' >= {min_population}.")
    print("Filtered data preview (first 5 rows):")
    # Use to_string for better console display in tool output
    print(df_filtered.head().to_string(index=False))

    # Save the filtered data to the specified CSV file
    df_filtered.to_csv(output_csv, index=False)
    print(f"Filtered data saved to '{output_csv}'")

except KeyError as e:
    print(f"Error: Missing expected column - {e}. Please check the CSV files for column names like '{population_column}'.")
    sys.exit(1)
except Exception as e:
    print(f"An unexpected error occurred: {e}")
    sys.exit(1)
