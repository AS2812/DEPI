# %% [markdown]
# # MTA Daily Ridership Data Cleaning and Preprocessing Notebook
# 
# **Project Overview:**
# 
# This notebook cleans and preprocesses the MTA Daily Ridership dataset for analysis and forecasting.
# It mimics the SQL cleaning process which:
# - Drops rows missing critical values,
# - Converts string dates to DATE,
# - Adjusts numeric values (setting NULL/negative values to 0, capping extreme values),
# - Trims extra spaces,
# - Removes duplicate records.
#
# The final cleaned dataset is exported as a CSV file.

# %% [code]
import pandas as pd
import numpy as np
import os
from IPython.display import FileLink, display

# %% [markdown]
# ## Step 1: Load the Raw CSV Data
# 
# The raw CSV file is assumed to be located at:
# `C:\Users\karim\Downloads\MTA_Daily_Ridership.csv`

# %% [code]
input_csv_path = r"C:\Users\karim\Downloads\MTA_Daily_Ridership.csv"

# Read the CSV file
df_raw = pd.read_csv(input_csv_path)
print("Raw data loaded successfully from", input_csv_path)
print("Raw Data Preview:")
display(df_raw.head())

# %% [markdown]
# ## Step 2: Map Raw Columns to Staging Table Format
# 
# We create a staging DataFrame with the following columns:
# - **ride_date_str**: from "Date"
# - **station_id**: Since the raw data is systemwide, we assign a dummy value (1).
# - **daily_ridership**: Sum of all mode-specific ridership columns.
# - **raw_text_field**: A constant value ("MTA Aggregate").
# - **subway_ridership**: from "Subways: Total Estimated Ridership"
# - **bus_ridership**: from "Buses: Total Estimated Ridership"
# - **lirr_ridership**: from "LIRR: Total Estimated Ridership"
# - **metro_north_ridership**: from "Metro-North: Total Estimated Ridership"
# - **access_a_ride_ridership**: from "Access-A-Ride: Total Estimated Ridership" (if present; otherwise 0)
# - **bridges_tunnels_ridership**: from "Bridges & Tunnels: Total Estimated Ridership" (if present; otherwise 0)
# - **pre_pandemic_comparison**: from "Staten Island Railway: % of Comparable Pre-Pandemic Day"
#
# For daily_ridership, we add the mode-specific values plus the Staten Island Railway ridership.

# %% [code]
df_stage = pd.DataFrame()

# Map the date column from "Date"
df_stage['ride_date_str'] = df_raw['Date']

# Assign a dummy station_id (since the data is systemwide)
df_stage['station_id'] = 1

# Map mode-specific ridership columns (if a column is missing, use 0)
df_stage['subway_ridership'] = df_raw.get("Subways: Total Estimated Ridership", 0)
df_stage['bus_ridership'] = df_raw.get("Buses: Total Estimated Ridership", 0)
df_stage['lirr_ridership'] = df_raw.get("LIRR: Total Estimated Ridership", 0)
df_stage['metro_north_ridership'] = df_raw.get("Metro-North: Total Estimated Ridership", 0)
df_stage['access_a_ride_ridership'] = df_raw.get("Access-A-Ride: Total Estimated Ridership", 0)
df_stage['bridges_tunnels_ridership'] = df_raw.get("Bridges & Tunnels: Total Estimated Ridership", 0)

# For Staten Island Railway ridership, we assume the column exists
siren_col = "Staten Island Railway: Total Estimated Ridership"
if siren_col in df_raw.columns:
    siren_ridership = df_raw[siren_col]
else:
    siren_ridership = 0

# Calculate daily_ridership as the sum of all mode-specific columns plus Staten Island Railway ridership
df_stage['daily_ridership'] = (df_stage['subway_ridership'] + df_stage['bus_ridership'] +
                               df_stage['lirr_ridership'] + df_stage['metro_north_ridership'] +
                               df_stage['access_a_ride_ridership'] + df_stage['bridges_tunnels_ridership'] +
                               siren_ridership)

# Set a constant raw_text_field
df_stage['raw_text_field'] = "MTA Aggregate"

# Map pre_pandemic_comparison from Staten Island Railway percentage column
pct_col = "Staten Island Railway: % of Comparable Pre-Pandemic Day"
df_stage['pre_pandemic_comparison'] = df_raw.get(pct_col, 0)

print("Staging Data Preview:")
display(df_stage.head())

# %% [markdown]
# ## Step 3: Clean and Transform the Data
# 
# Cleaning Steps (mimicking the SQL script):
# 1. Drop rows with missing ride_date_str.
# 2. Convert ride_date_str to a proper date (ride_date).
# 3. Clean numeric fields:
#    - For ridership columns, replace NULL/NaN or negative values with 0 and cap values above 100000.
# 4. Clean pre_pandemic_comparison: replace NULL/NaN or negative values with 0 and cap values above 200.
# 5. Trim extra spaces in raw_text_field.
# 6. Remove duplicate records based on (ride_date, station_id).

# %% [code]
# 1. Remove rows missing ride_date_str
df_stage = df_stage.dropna(subset=['ride_date_str'])

# 2. Convert ride_date_str to datetime column 'ride_date'
df_stage['ride_date'] = pd.to_datetime(df_stage['ride_date_str'], errors='coerce')
df_stage = df_stage.dropna(subset=['ride_date'])

# Define cleaning functions
def clean_numeric(series, cap=100000):
    """Replace NaN and negative values with 0 and cap values above 'cap'."""
    series = series.fillna(0)
    series = series.apply(lambda x: 0 if x < 0 else x)
    series = series.apply(lambda x: cap if x > cap else x)
    return series

def clean_percentage(series, cap=200):
    """Replace NaN and negative percentage values with 0 and cap values above 'cap'."""
    series = series.fillna(0)
    series = series.apply(lambda x: 0 if x < 0 else x)
    series = series.apply(lambda x: cap if x > cap else x)
    return series

# 3. Clean numeric columns for ridership
cols_to_clean = ['daily_ridership', 'subway_ridership', 'bus_ridership', 
                 'lirr_ridership', 'metro_north_ridership', 'access_a_ride_ridership',
                 'bridges_tunnels_ridership']
for col in cols_to_clean:
    df_stage[col] = clean_numeric(df_stage[col], cap=100000)

# 4. Clean pre_pandemic_comparison (cap at 200)
df_stage['pre_pandemic_comparison'] = clean_percentage(df_stage['pre_pandemic_comparison'], cap=200)

# 5. Trim extra spaces in raw_text_field
df_stage['raw_text_field'] = df_stage['raw_text_field'].str.strip()

# 6. Remove duplicate records based on (ride_date, station_id)
df_stage = df_stage.sort_values(by=['ride_date']).drop_duplicates(subset=['ride_date', 'station_id'], keep='first')

print("Cleaned Staging Data Preview:")
display(df_stage.head())

# %% [markdown]
# ## Step 4: Create Final Cleaned DataFrame
# 
# The final cleaned DataFrame will contain:
# - ride_date (DATE)
# - station_id (INT)
# - daily_ridership (INT)
# - raw_text_field (VARCHAR)
# - subway_ridership (INT)
# - bus_ridership (INT)
# - lirr_ridership (INT)
# - metro_north_ridership (INT)
# - access_a_ride_ridership (INT)
# - bridges_tunnels_ridership (INT)
# - pre_pandemic_comparison (REAL)

# %% [code]
final_columns = ['ride_date', 'station_id', 'daily_ridership', 'raw_text_field',
                 'subway_ridership', 'bus_ridership', 'lirr_ridership', 'metro_north_ridership',
                 'access_a_ride_ridership', 'bridges_tunnels_ridership', 'pre_pandemic_comparison']
df_final = df_stage[final_columns].copy()

print("Final Cleaned Data Preview:")
display(df_final.head())

# %% [markdown]
# ## Step 5: Export the Cleaned Data to CSV
# 
# The final cleaned dataset is exported to:
# `C:\Users\karim\Downloads\MTA_Ridership_Cleaned.csv`

# %% [code]
output_csv_path = r"C:\Users\karim\Downloads\MTA_Ridership_Cleaned.csv"
df_final.to_csv(output_csv_path, index=False)
print("Cleaned CSV file has been saved as:", output_csv_path)

# %% [markdown]
# ## Step 6: Download Link (Jupyter Notebook Environment)
# 
# If running in Jupyter Notebook, click the link below to download the cleaned CSV file.

# %% [code]
display(FileLink(output_csv_path))
