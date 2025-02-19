-----------------------------------------------------------------------
-- MTA Daily Ridership Data Cleaning and Preprocessing Script
-- Based on the MTA Daily Ridership Data Overview [&#8203;:contentReference[oaicite:0]{index=0}] and
-- the Data Cleaning Process [&#8203;:contentReference[oaicite:1]{index=1}].
--
-- This script performs the following:
--   • Drops existing staging and cleaned tables.
--   • Creates a staging table that simulates raw CSV input.
--   • Inserts sample data that reflects common issues:
--         - Missing values (ride_date_str, station_id, daily_ridership, etc.)
--         - Negative or out-of-range numeric values.
--         - Extra spaces in text fields.
--         - Duplicate records.
--         - Additional columns for various ridership modes and pre-pandemic comparison.
--   • Creates a final cleaned table with proper data types and a composite primary key.
--   • Cleans the data by:
--         - Converting string dates to DATE.
--         - Filtering out rows missing critical values.
--         - Adjusting numeric values (setting NULL/negative values to 0 and capping extreme values).
--         - Trimming extra spaces.
--         - Removing duplicate records using a window function.
--   • Creates indexes for performance.
--   • Performs data quality checks.
--
-- NOTE:
-- The MTA Daily Ridership dataset covers multiple agencies (NYCT, MTABC, LIRR, Metro-North,
-- Access-A-Ride, and Bridges & Tunnels). If your raw data includes further fields, adjust the schema accordingly.
-----------------------------------------------------------------------

-----------------------------------------------------------------------
-- DROP EXISTING TABLES (if they exist)
-----------------------------------------------------------------------
DROP TABLE IF EXISTS MTA_Ridership_Staging;
DROP TABLE IF EXISTS MTA_Ridership_Cleaned;

-----------------------------------------------------------------------
-- 1. CREATE A STAGING TABLE TO IMPORT THE RAW CSV DATA
--    (Assume the CSV data is imported with ride_date stored as a string)
-----------------------------------------------------------------------
CREATE TABLE MTA_Ridership_Staging (
    ride_date_str              VARCHAR(50),  -- Raw ride date as a string (e.g., '2025-02-01')
    station_id                 INT,          -- Station identifier (critical for uniqueness)
    daily_ridership            INT,          -- Overall daily ridership count
    raw_text_field             VARCHAR(255), -- A text field that may have extra spaces or formatting issues
    subway_ridership           INT,          -- Subway ridership count
    bus_ridership              INT,          -- Bus ridership count
    lirr_ridership             INT,          -- Long Island Rail Road ridership count
    metro_north_ridership      INT,          -- Metro-North Railroad ridership count
    access_a_ride_ridership    INT,          -- Access-A-Ride ridership count
    bridges_tunnels_ridership  INT,          -- Bridges and Tunnels ridership/traffic count
    pre_pandemic_comparison    REAL          -- Pre-pandemic comparison percentage (e.g., 95.50)
    -- Additional columns can be added as needed.
);

-----------------------------------------------------------------------
-- Insert sample data into the staging table.
-- The sample rows simulate various data issues:
--   • Row 1: A valid row with extra spaces in text fields.
--   • Row 2: Missing ride_date_str (will be excluded).
--   • Row 3: Missing station_id (will be excluded).
--   • Row 4: Negative ridership values (will be set to 0).
--   • Row 5 & 6: Ridership values above threshold and duplicate record (capped and de-duplicated).
--   • Row 7: Missing ridership values (will be set to 0).
-----------------------------------------------------------------------
INSERT INTO MTA_Ridership_Staging 
  (ride_date_str, station_id, daily_ridership, raw_text_field, subway_ridership, bus_ridership, lirr_ridership, metro_north_ridership, access_a_ride_ridership, bridges_tunnels_ridership, pre_pandemic_comparison)
VALUES
  ('2025-02-01', 101, 5000, '  Station A  ', 3000, 2000, 0, 0, 0, 0, 95.50),
  (NULL,         102, 3000, 'Station B',       2500, 500,  NULL, NULL, NULL, NULL, 98.00),          -- Missing ride_date_str; filtered out.
  ('2025-02-03', NULL, 4500, 'Station C ',      4000, 500,  300, 200, 100, 50, 97.25),          -- Missing station_id; filtered out.
  ('2025-02-04', 103, -50,  ' Station D',      -100, 0,    0, 0, 0, 0, -5.00),            -- Negative values; set to 0.
  ('2025-02-05', 104, 150000, 'Station E',      80000, 70000, 50000, 60000, 40000, 30000, 120.75), -- Exceeds threshold; capped.
  ('2025-02-05', 104, 150000, 'Station E',      80000, 70000, 50000, 60000, 40000, 30000, 120.75), -- Duplicate record.
  ('2025-02-06', 105, NULL,   ' Station F',     NULL,  NULL,  NULL,  NULL,  NULL,  NULL, NULL);   -- Missing ridership; set to 0.

-----------------------------------------------------------------------
-- 2. CREATE THE FINAL (CLEANED) TABLE WITH PROPER DATA TYPES AND CONSTRAINTS
--    This table stores the cleaned data ready for analysis.
-----------------------------------------------------------------------
CREATE TABLE MTA_Ridership_Cleaned (
    ride_date                 DATE NOT NULL,      -- Cleaned ride date (DATE type)
    station_id                INT NOT NULL,       -- Station identifier
    daily_ridership           INT,                -- Overall cleaned daily ridership count
    raw_text_field            VARCHAR(255),       -- Cleaned text field (trimmed)
    subway_ridership          INT,                -- Cleaned subway ridership count
    bus_ridership             INT,                -- Cleaned bus ridership count
    lirr_ridership            INT,                -- Cleaned LIRR ridership count
    metro_north_ridership     INT,                -- Cleaned Metro-North ridership count
    access_a_ride_ridership   INT,                -- Cleaned Access-A-Ride ridership count
    bridges_tunnels_ridership INT,                -- Cleaned Bridges & Tunnels ridership count
    pre_pandemic_comparison   REAL,               -- Cleaned pre-pandemic comparison percentage
    PRIMARY KEY (ride_date, station_id)
    -- Extend the schema if additional cleaned fields are required.
);

-----------------------------------------------------------------------
-- 3. DELETE ANY EXISTING RECORDS FROM THE CLEANED TABLE
--    (Ensure the cleaned table is empty before new data is inserted.)
-----------------------------------------------------------------------
DELETE FROM MTA_Ridership_Cleaned;

-----------------------------------------------------------------------
-- 4. CLEAN & INSERT DATA INTO THE FINAL TABLE WITH DUPLICATE REMOVAL
--
-- This step performs the following cleaning operations:
--   a) Converts ride_date_str to a proper DATE (rows with missing/empty ride_date_str are excluded).
--   b) Filters out rows with missing station_id.
--   c) Cleans numeric fields:
--         - For overall and mode-specific ridership values: 
--             * If NULL, set to 0.
--             * If negative, set to 0.
--             * If above 100000, cap at 100000.
--         - For pre_pandemic_comparison:
--             * If NULL, set to 0.
--             * If negative, set to 0.
--             * If above 200, cap at 200.
--   d) Trims extra spaces from raw_text_field.
--   e) Uses a window function (ROW_NUMBER()) to remove duplicate records.
-----------------------------------------------------------------------
WITH CleanedData AS (
    SELECT
        CAST(ride_date_str AS DATE) AS ride_date,
        station_id,
        -- Clean overall daily ridership
        CASE
            WHEN daily_ridership IS NULL THEN 0
            WHEN daily_ridership < 0 THEN 0
            WHEN daily_ridership > 100000 THEN 100000
            ELSE daily_ridership
        END AS daily_ridership,
        TRIM(raw_text_field) AS raw_text_field,
        -- Clean subway ridership
        CASE
            WHEN subway_ridership IS NULL THEN 0
            WHEN subway_ridership < 0 THEN 0
            WHEN subway_ridership > 100000 THEN 100000
            ELSE subway_ridership
        END AS subway_ridership,
        -- Clean bus ridership
        CASE
            WHEN bus_ridership IS NULL THEN 0
            WHEN bus_ridership < 0 THEN 0
            WHEN bus_ridership > 100000 THEN 100000
            ELSE bus_ridership
        END AS bus_ridership,
        -- Clean LIRR ridership
        CASE
            WHEN lirr_ridership IS NULL THEN 0
            WHEN lirr_ridership < 0 THEN 0
            WHEN lirr_ridership > 100000 THEN 100000
            ELSE lirr_ridership
        END AS lirr_ridership,
        -- Clean Metro-North ridership
        CASE
            WHEN metro_north_ridership IS NULL THEN 0
            WHEN metro_north_ridership < 0 THEN 0
            WHEN metro_north_ridership > 100000 THEN 100000
            ELSE metro_north_ridership
        END AS metro_north_ridership,
        -- Clean Access-A-Ride ridership
        CASE
            WHEN access_a_ride_ridership IS NULL THEN 0
            WHEN access_a_ride_ridership < 0 THEN 0
            WHEN access_a_ride_ridership > 100000 THEN 100000
            ELSE access_a_ride_ridership
        END AS access_a_ride_ridership,
        -- Clean Bridges & Tunnels ridership
        CASE
            WHEN bridges_tunnels_ridership IS NULL THEN 0
            WHEN bridges_tunnels_ridership < 0 THEN 0
            WHEN bridges_tunnels_ridership > 100000 THEN 100000
            ELSE bridges_tunnels_ridership
        END AS bridges_tunnels_ridership,
        -- Clean pre-pandemic comparison: cap values above 200.
        CASE
            WHEN pre_pandemic_comparison IS NULL THEN 0
            WHEN pre_pandemic_comparison < 0 THEN 0
            WHEN pre_pandemic_comparison > 200 THEN 200
            ELSE pre_pandemic_comparison
        END AS pre_pandemic_comparison,
        -- Assign a row number for duplicate removal
        ROW_NUMBER() OVER (
            PARTITION BY CAST(ride_date_str AS DATE), station_id
            ORDER BY ride_date_str
        ) AS rn
    FROM MTA_Ridership_Staging
    WHERE station_id IS NOT NULL
      AND ride_date_str IS NOT NULL
      AND ride_date_str <> ''
)
INSERT INTO MTA_Ridership_Cleaned 
    (ride_date, station_id, daily_ridership, raw_text_field, subway_ridership, bus_ridership, lirr_ridership, metro_north_ridership, access_a_ride_ridership, bridges_tunnels_ridership, pre_pandemic_comparison)
SELECT
    ride_date,
    station_id,
    daily_ridership,
    raw_text_field,
    subway_ridership,
    bus_ridership,
    lirr_ridership,
    metro_north_ridership,
    access_a_ride_ridership,
    bridges_tunnels_ridership,
    pre_pandemic_comparison
FROM CleanedData
WHERE rn = 1;

-----------------------------------------------------------------------
-- 5. ADDITIONAL CLEANING STEPS & PERFORMANCE OPTIMIZATIONS
-----------------------------------------------------------------------
-- a) Conversion of string dates is handled above.
    
-- b) Create indexes on key columns to improve query performance on filtering and joins.
CREATE INDEX idx_station_id ON MTA_Ridership_Cleaned(station_id);
CREATE INDEX idx_ride_date  ON MTA_Ridership_Cleaned(ride_date);

-- c) Example: Retrieve all records where overall daily ridership is above the average.
SELECT *
FROM MTA_Ridership_Cleaned
WHERE daily_ridership > (
    SELECT AVG(daily_ridership)
    FROM MTA_Ridership_Cleaned
);

-----------------------------------------------------------------------
-- 6. ADDITIONAL DATA QUALITY CHECKS
-----------------------------------------------------------------------
-- 6.1 Verify that no records have a default ride_date ('1900-01-01').
--     (Since rows with missing ride_date_str are filtered out, this should return no rows.)
SELECT *
FROM MTA_Ridership_Cleaned
WHERE ride_date = '1900-01-01';

-- 6.2 Ensure that text fields are properly trimmed (i.e., no leading or trailing whitespace).
SELECT *
FROM MTA_Ridership_Cleaned
WHERE raw_text_field LIKE ' %' OR raw_text_field LIKE '% ';

-- 6.3 Identify records with anomalous ridership values (e.g., 0 or capped at 100000).
SELECT *
FROM MTA_Ridership_Cleaned
WHERE daily_ridership = 0 OR daily_ridership = 100000;

-----------------------------------------------------------------------
-- END OF SCRIPT
-----------------------------------------------------------------------
