import pandas as pd
import numpy as np

# =========================
# Step 1: Load Raw CSV Data
# =========================
input_csv_path = r"C:\Users\karim\Downloads\MTA_Daily_Ridership.csv"
df_raw = pd.read_csv(input_csv_path)

print("Raw data loaded successfully:")
print(df_raw.head())
print(df_raw.info())

# ================================
# Step 2: Rename & Pre-Process Columns
# ================================
# Standardize column names: remove spaces, colons, special characters; convert to lowercase.
df_raw.columns = (
    df_raw.columns.str.strip()              # Remove leading/trailing spaces
             .str.replace(" ", "_")         # Replace spaces with underscores
             .str.replace(":", "")          # Remove colons
             .str.lower()                   # Convert to lowercase
)

print("Updated Columns:")
print(df_raw.columns)

# Convert the 'date' column to datetime format
df_raw['date'] = pd.to_datetime(df_raw['date'], errors='coerce')

# Convert percentage columns from int to float for precision
percentage_columns = [
    'subways_percent_of_comparable_pre_pandemic_day',
    'buses_percent_of_comparable_pre_pandemic_day',
    'lirr_percent_of_comparable_pre_pandemic_day',
    'metro_north_percent_of_comparable_pre_pandemic_day',
    'access_a_ride_percent_of_comparable_pre_pandemic_day',
    'bridges_and_tunnels_percent_of_comparable_pre_pandemic_day',
    'staten_island_railway_percent_of_comparable_pre_pandemic_day'
]
df_raw[percentage_columns] = df_raw[percentage_columns].astype(float)

# Fill any missing values and remove duplicate rows
df_raw.fillna(0, inplace=True)
df_raw.drop_duplicates(inplace=True)

# ============================
# Step 3: Map to a Staging DataFrame
# ============================
# We create a staging DataFrame that mimics a SQL staging table structure.
df_stage = pd.DataFrame()

# Map raw date to a staging column (as a string)
df_stage['ride_date_str'] = df_raw['date'].astype(str)

# Since the data is systemwide, assign a dummy station_id
df_stage['station_id'] = 1

# Map mode-specific ridership columns; if a column is missing, default to 0
df_stage['subway_ridership'] = df_raw.get("subways_total_estimated_ridership", 0)
df_stage['bus_ridership'] = df_raw.get("buses_total_estimated_ridership", 0)
df_stage['lirr_ridership'] = df_raw.get("lirr_total_estimated_ridership", 0)
df_stage['metro_north_ridership'] = df_raw.get("metro_north_total_estimated_ridership", 0)
df_stage['access_a_ride_ridership'] = df_raw.get("access_a_ride_total_scheduled_trips", 0)
df_stage['bridges_tunnels_ridership'] = df_raw.get("bridges_and_tunnels_total_traffic", 0)

# For Staten Island Railway ridership, check if the column exists
siren_col = "staten_island_railway_total_estimated_ridership"
if siren_col in df_raw.columns:
    siren_ridership = df_raw[siren_col]
else:
    siren_ridership = 0

# Calculate daily ridership as the sum of all mode-specific columns plus Staten Island Railway ridership
df_stage['daily_ridership'] = (
    df_stage['subway_ridership'] +
    df_stage['bus_ridership'] +
    df_stage['lirr_ridership'] +
    df_stage['metro_north_ridership'] +
    df_stage['access_a_ride_ridership'] +
    df_stage['bridges_tunnels_ridership'] +
    siren_ridership
)

# Set a constant value for raw text field
df_stage['raw_text_field'] = "MTA Aggregate"

# Map pre-pandemic comparison from Staten Island Railway percentage column
pct_col = "staten_island_railway_percent_of_comparable_pre_pandemic_day"
df_stage['pre_pandemic_comparison'] = df_raw.get(pct_col, 0)

print("Staging Data Preview:")
print(df_stage.head())

# ===============================
# Step 4: Clean & Transform the Data
# ===============================
# 1. Remove rows missing ride_date_str
df_stage = df_stage.dropna(subset=['ride_date_str'])

# 2. Convert ride_date_str to a proper datetime column 'ride_date'
df_stage['ride_date'] = pd.to_datetime(df_stage['ride_date_str'], errors='coerce')
df_stage = df_stage.dropna(subset=['ride_date'])

# Define cleaning functions for numeric and percentage fields
def clean_numeric(series, cap=100000):
    """Replace NaN and negative values with 0 and cap values above a threshold."""
    series = series.fillna(0)
    series = series.apply(lambda x: 0 if x < 0 else x)
    series = series.apply(lambda x: cap if x > cap else x)
    return series

def clean_percentage(series, cap=200):
    """Replace NaN and negative percentage values with 0 and cap values above a threshold."""
    series = series.fillna(0)
    series = series.apply(lambda x: 0 if x < 0 else x)
    series = series.apply(lambda x: cap if x > cap else x)
    return series

# Clean numeric ridership columns
numeric_cols = [
    'daily_ridership', 'subway_ridership', 'bus_ridership',
    'lirr_ridership', 'metro_north_ridership', 'access_a_ride_ridership',
    'bridges_tunnels_ridership'
]
for col in numeric_cols:
    df_stage[col] = clean_numeric(df_stage[col], cap=100000)

# Clean the pre-pandemic comparison percentage column (cap at 200)
df_stage['pre_pandemic_comparison'] = clean_percentage(df_stage['pre_pandemic_comparison'], cap=200)

# Trim extra spaces from the raw_text_field
df_stage['raw_text_field'] = df_stage['raw_text_field'].str.strip()

# Remove duplicate records based on ride_date and station_id
df_stage = df_stage.sort_values(by=['ride_date']).drop_duplicates(subset=['ride_date', 'station_id'], keep='first')

print("Cleaned Staging Data Preview:")
print(df_stage.head())

# =====================================
# Step 5: Create the Final Cleaned DataFrame
# =====================================
final_columns = [
    'ride_date', 'station_id', 'daily_ridership', 'raw_text_field',
    'subway_ridership', 'bus_ridership', 'lirr_ridership', 'metro_north_ridership',
    'access_a_ride_ridership', 'bridges_tunnels_ridership', 'pre_pandemic_comparison'
]
df_final = df_stage[final_columns].copy()

print("Final Cleaned Data Preview:")
print(df_final.head())

# ===============================
# Step 6: Export the Cleaned Data to CSV
# ===============================
output_csv_path = r"C:\Users\karim\Downloads\MTA_Ridership_Cleaned.csv"
df_final.to_csv(output_csv_path, index=False)
print("Cleaned CSV file has been saved as:", output_csv_path)
