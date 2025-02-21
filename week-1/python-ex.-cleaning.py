import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


# =============================================================================
# Step 1: Data Loading & Setup
# =============================================================================
input_csv_path = r"C:\Users\karim\Downloads\MTA_Daily_Ridership.csv"
df_raw = pd.read_csv(input_csv_path)

print("Raw data loaded successfully:")
print(df_raw.head())
print(df_raw.info())

# =============================================================================
# Step 2: Rename & Pre-Process Columns
# =============================================================================
# Standardize column names: remove extra spaces, replace spaces with underscores,
# remove colons, and convert to lowercase.
df_raw.columns = (
    df_raw.columns.str.strip()        # Remove leading/trailing spaces
               .str.replace(" ", "_")  # Replace spaces with underscores
               .str.replace(":", "")   # Remove colons
               .str.lower()            # Convert to lowercase
)

print("Updated Columns:")
print(df_raw.columns)

# Convert the 'date' column to datetime (errors coerced to NaT)
df_raw['date'] = pd.to_datetime(df_raw['date'], errors='coerce')

# Update percentage_columns list to match the renamed columns
percentage_columns = [
    'subways_%_of_comparable_pre-pandemic_day',
    'buses_%_of_comparable_pre-pandemic_day',
    'lirr_%_of_comparable_pre-pandemic_day',
    'metro-north_%_of_comparable_pre-pandemic_day',
    'access-a-ride_%_of_comparable_pre-pandemic_day',
    'bridges_and_tunnels_%_of_comparable_pre-pandemic_day',
    'staten_island_railway_%_of_comparable_pre-pandemic_day'
]

# Convert percentage columns from int to float for precision
df_raw[percentage_columns] = df_raw[percentage_columns].astype(float)

# Fill missing values with 0 and remove duplicate rows
df_raw.fillna(0, inplace=True)
df_raw.drop_duplicates(inplace=True)

# =============================================================================
# Step 3: Map to a Staging DataFrame
# =============================================================================
# Create a staging DataFrame that mimics a SQL staging table structure.
df_stage = pd.DataFrame()

# Map raw date as a string for staging and assign a dummy station_id
df_stage['ride_date_str'] = df_raw['date'].astype(str)
df_stage['station_id'] = 1

# Map mode-specific ridership columns (default to 0 if missing)
df_stage['subway_ridership'] = df_raw.get("subways_total_estimated_ridership", 0)
df_stage['bus_ridership'] = df_raw.get("buses_total_estimated_ridership", 0)
df_stage['lirr_ridership'] = df_raw.get("lirr_total_estimated_ridership", 0)
df_stage['metro_north_ridership'] = df_raw.get("metro-north_total_estimated_ridership", 0)
df_stage['access_a_ride_ridership'] = df_raw.get("access-a-ride_total_scheduled_trips", 0)
df_stage['bridges_tunnels_ridership'] = df_raw.get("bridges_and_tunnels_total_traffic", 0)

# For Staten Island Railway ridership, check if the column exists
siren_col = "staten_island_railway_total_estimated_ridership"
if siren_col in df_raw.columns:
    siren_ridership = df_raw[siren_col]
else:
    siren_ridership = 0

# Calculate daily ridership as the sum of all mode-specific columns plus Staten Island Railway
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

# Map pre-pandemic comparison using Staten Island Railway percentage column (default to 0)
pct_col = "staten_island_railway_%_of_comparable_pre-pandemic_day"
df_stage['pre_pandemic_comparison'] = df_raw.get(pct_col, 0)

print("Staging Data Preview:")
print(df_stage.head())

# =============================================================================
# Step 4: Clean & Transform the Staging Data
# =============================================================================
# Remove rows with missing ride_date_str and convert to proper datetime column
df_stage = df_stage.dropna(subset=['ride_date_str'])
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

# Clean the pre-pandemic comparison percentage column
df_stage['pre_pandemic_comparison'] = clean_percentage(df_stage['pre_pandemic_comparison'], cap=200)

# Trim extra spaces from the raw_text_field
df_stage['raw_text_field'] = df_stage['raw_text_field'].str.strip()

# Remove duplicate records based on ride_date and station_id
df_stage = df_stage.sort_values(by=['ride_date']).drop_duplicates(subset=['ride_date', 'station_id'], keep='first')

print("Cleaned Staging Data Preview:")
print(df_stage.head())

# =============================================================================
# Step 5: Create the Final Cleaned DataFrame
# =============================================================================
final_columns = [
    'ride_date', 'station_id', 'daily_ridership', 'raw_text_field',
    'subway_ridership', 'bus_ridership', 'lirr_ridership', 'metro_north_ridership',
    'access_a_ride_ridership', 'bridges_tunnels_ridership', 'pre_pandemic_comparison'
]
df_final = df_stage[final_columns].copy()

print("Final Cleaned Data Preview:")
print(df_final.head())

# =============================================================================
# Step 6: Data Exploration & Statistical Analysis
# =============================================================================
# Generate descriptive statistics for the cleaned data
stats_summary = df_final.describe()
print("Descriptive Statistics:")
print(stats_summary)

# Calculate mode values (mode() might return multiple rows; we take the first)
mode_values = df_final.mode().iloc[0]
print("Mode values:")
print(mode_values)



# =============================================================================
# Step 7: Final Output & Reporting
# =============================================================================
# Export the final cleaned DataFrame to a CSV file
output_csv_path = r"C:\Users\karim\Downloads\MTA_Ridership_Cleaned.csv"
df_final.to_csv(output_csv_path, index=False)
print("Cleaned CSV file has been saved as:", output_csv_path)

# Optionally, write a summary report to a text file
with open('mta_data_cleaning_report.txt', 'w') as f:
    f.write("MTA Data Cleaning Report\n")
    f.write("========================\n\n")
    f.write("Descriptive Statistics:\n")
    f.write(stats_summary.to_string())
    f.write("\n\nMode Values:\n")
    f.write(mode_values.to_string())

print("Data cleaning report has been saved as mta_data_cleaning_report.txt.")
