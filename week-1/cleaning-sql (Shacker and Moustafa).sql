-- ============================================================
-- SQLite Script: Clean & Retrieve Data from MTA_Daily_Ridership
--
-- Overview:
--   This script performs data cleaning on the MTA_Daily_Ridership table.
--   It:
--     1. Reads the raw data from the table.
--     2. Converts the "Date" column from TEXT to a proper DATE.
--     3. Cleans numeric columns by:
--          - Setting NULL or negative values to 0.
--          - Capping extremely high values (e.g., ridership > 100000, percentages > 200).
--     4. Optionally removes duplicate rows based on the date.
--     5. Provides optional data quality checks.
--
-- Further cleaning considerations:
--   - If there are any text columns (e.g., station names), consider using TRIM() 
--     and standardizing the case (UPPER() or LOWER()).
--   - Review the thresholds (100000 for counts, 200 for percentages) to ensure they
--     suit your specific data context.
--   - For more advanced cleaning (e.g., handling missing values differently or normalizing data),
--     you might consider using Python with Pandas.
-- ============================================================

WITH CleanedData AS (
    SELECT
        -- Convert "Date" from TEXT to a DATE type.
        DATE("Date") AS ride_date,

        -- 1) Clean "Subways:_Total_Estimated_Ridership":
        --    - Replace NULL or negative values with 0.
        --    - Cap values above 100,000.
        CASE
            WHEN "Subways:_Total_Estimated_Ridership" IS NULL 
                 OR "Subways:_Total_Estimated_Ridership" < 0 THEN 0
            WHEN "Subways:_Total_Estimated_Ridership" > 100000 THEN 100000
            ELSE "Subways:_Total_Estimated_Ridership"
        END AS subways_ridership,

        -- 2) Clean "Subways:_%_of_Comparable_Pre-Pandemic_Day":
        --    - Replace NULL or negative values with 0.
        --    - Cap values above 200.
        CASE
            WHEN "Subways:_%_of_Comparable_Pre-Pandemic_Day" IS NULL 
                 OR "Subways:_%_of_Comparable_Pre-Pandemic_Day" < 0 THEN 0
            WHEN "Subways:_%_of_Comparable_Pre-Pandemic_Day" > 200 THEN 200
            ELSE "Subways:_%_of_Comparable_Pre-Pandemic_Day"
        END AS subways_pct_of_pre,

        -- 3) Clean "Buses:_Total_Estimated_Ridership":
        CASE
            WHEN "Buses:_Total_Estimated_Ridership" IS NULL 
                 OR "Buses:_Total_Estimated_Ridership" < 0 THEN 0
            WHEN "Buses:_Total_Estimated_Ridership" > 100000 THEN 100000
            ELSE "Buses:_Total_Estimated_Ridership"
        END AS buses_ridership,

        -- 4) Clean "Buses:_%_of_Comparable_Pre-Pandemic_Day":
        CASE
            WHEN "Buses:_%_of_Comparable_Pre-Pandemic_Day" IS NULL 
                 OR "Buses:_%_of_Comparable_Pre-Pandemic_Day" < 0 THEN 0
            WHEN "Buses:_%_of_Comparable_Pre-Pandemic_Day" > 200 THEN 200
            ELSE "Buses:_%_of_Comparable_Pre-Pandemic_Day"
        END AS buses_pct_of_pre,

        -- 5) Clean "LIRR:_Total_Estimated_Ridership":
        CASE
            WHEN "LIRR:_Total_Estimated_Ridership" IS NULL 
                 OR "LIRR:_Total_Estimated_Ridership" < 0 THEN 0
            WHEN "LIRR:_Total_Estimated_Ridership" > 100000 THEN 100000
            ELSE "LIRR:_Total_Estimated_Ridership"
        END AS lirr_ridership,

        -- 6) Clean "LIRR:_%_of_Comparable_Pre-Pandemic_Day":
        CASE
            WHEN "LIRR:_%_of_Comparable_Pre-Pandemic_Day" IS NULL 
                 OR "LIRR:_%_of_Comparable_Pre-Pandemic_Day" < 0 THEN 0
            WHEN "LIRR:_%_of_Comparable_Pre-Pandemic_Day" > 200 THEN 200
            ELSE "LIRR:_%_of_Comparable_Pre-Pandemic_Day"
        END AS lirr_pct_of_pre,

        -- 7) Clean "Metro-North:_Total_Estimated_Ridership":
        CASE
            WHEN "Metro-North:_Total_Estimated_Ridership" IS NULL 
                 OR "Metro-North:_Total_Estimated_Ridership" < 0 THEN 0
            WHEN "Metro-North:_Total_Estimated_Ridership" > 100000 THEN 100000
            ELSE "Metro-North:_Total_Estimated_Ridership"
        END AS mnr_ridership,

        -- 8) Clean "Metro-North:_%_of_Comparable_Pre-Pandemic_Day":
        CASE
            WHEN "Metro-North:_%_of_Comparable_Pre-Pandemic_Day" IS NULL 
                 OR "Metro-North:_%_of_Comparable_Pre-Pandemic_Day" < 0 THEN 0
            WHEN "Metro-North:_%_of_Comparable_Pre-Pandemic_Day" > 200 THEN 200
            ELSE "Metro-North:_%_of_Comparable_Pre-Pandemic_Day"
        END AS mnr_pct_of_pre,

        -- 9) Clean "Access-A-Ride:_Total_Scheduled_Trips":
        CASE
            WHEN "Access-A-Ride:_Total_Scheduled_Trips" IS NULL 
                 OR "Access-A-Ride:_Total_Scheduled_Trips" < 0 THEN 0
            WHEN "Access-A-Ride:_Total_Scheduled_Trips" > 100000 THEN 100000
            ELSE "Access-A-Ride:_Total_Scheduled_Trips"
        END AS aar_scheduled_trips,

        -- 10) Clean "Access-A-Ride:_%_of_Comparable_Pre-Pandemic_Day":
        CASE
            WHEN "Access-A-Ride:_%_of_Comparable_Pre-Pandemic_Day" IS NULL 
                 OR "Access-A-Ride:_%_of_Comparable_Pre-Pandemic_Day" < 0 THEN 0
            WHEN "Access-A-Ride:_%_of_Comparable_Pre-Pandemic_Day" > 200 THEN 200
            ELSE "Access-A-Ride:_%_of_Comparable_Pre-Pandemic_Day"
        END AS aar_pct_of_pre,

        -- 11) Clean "Bridges_and_Tunnels:_Total_Traffic":
        CASE
            WHEN "Bridges_and_Tunnels:_Total_Traffic" IS NULL 
                 OR "Bridges_and_Tunnels:_Total_Traffic" < 0 THEN 0
            WHEN "Bridges_and_Tunnels:_Total_Traffic" > 100000 THEN 100000
            ELSE "Bridges_and_Tunnels:_Total_Traffic"
        END AS bridges_tunnels_traffic,

        -- 12) Clean "Bridges_and_Tunnels:_%_of_Comparable_Pre-Pandemic_Day":
        CASE
            WHEN "Bridges_and_Tunnels:_%_of_Comparable_Pre-Pandemic_Day" IS NULL 
                 OR "Bridges_and_Tunnels:_%_of_Comparable_Pre-Pandemic_Day" < 0 THEN 0
            WHEN "Bridges_and_Tunnels:_%_of_Comparable_Pre-Pandemic_Day" > 200 THEN 200
            ELSE "Bridges_and_Tunnels:_%_of_Comparable_Pre-Pandemic_Day"
        END AS bridges_tunnels_pct_of_pre,

        -- 13) Clean "Staten_Island_Railway:_Total_Estimated_Ridership":
        CASE
            WHEN "Staten_Island_Railway:_Total_Estimated_Ridership" IS NULL 
                 OR "Staten_Island_Railway:_Total_Estimated_Ridership" < 0 THEN 0
            WHEN "Staten_Island_Railway:_Total_Estimated_Ridership" > 100000 THEN 100000
            ELSE "Staten_Island_Railway:_Total_Estimated_Ridership"
        END AS sir_ridership,

        -- 14) Clean "Staten_Island_Railway:_%_of_Comparable_Pre-Pandemic_Day":
        CASE
            WHEN "Staten_Island_Railway:_%_of_Comparable_Pre-Pandemic_Day" IS NULL 
                 OR "Staten_Island_Railway:_%_of_Comparable_Pre-Pandemic_Day" < 0 THEN 0
            WHEN "Staten_Island_Railway:_%_of_Comparable_Pre-Pandemic_Day" > 200 THEN 200
            ELSE "Staten_Island_Railway:_%_of_Comparable_Pre-Pandemic_Day"
        END AS sir_pct_of_pre,

        -- (Optional) Use ROW_NUMBER() to remove duplicate rows by date.
        -- This assigns a unique number (rn) to each row per date,
        -- so that we can later select only the first row (rn = 1).
        ROW_NUMBER() OVER (
            PARTITION BY DATE("Date")
            ORDER BY "Date"
        ) AS rn

    FROM MTA_Daily_Ridership
)

-- Select the cleaned and deduplicated data.
SELECT
    ride_date,
    subways_ridership,
    subways_pct_of_pre,
    buses_ridership,
    buses_pct_of_pre,
    lirr_ridership,
    lirr_pct_of_pre,
    mnr_ridership,
    mnr_pct_of_pre,
    aar_scheduled_trips,
    aar_pct_of_pre,
    bridges_tunnels_traffic,
    bridges_tunnels_pct_of_pre,
    sir_ridership,
    sir_pct_of_pre
FROM CleanedData
WHERE rn = 1;  -- Keep only the first row per date

-- ============================================================
-- OPTIONAL: DATA QUALITY CHECKS
-- ============================================================

-- Quality Check 1:
-- Find records where the date conversion might have produced an erroneous date (e.g., '1900-01-01').
SELECT *
FROM MTA_Daily_Ridership
WHERE DATE("Date") = '1900-01-01';

-- Quality Check 2:
-- Identify records in the raw table with negative values in key numeric columns.
SELECT *
FROM MTA_Daily_Ridership
WHERE "Subways:_Total_Estimated_Ridership" < 0
   OR "Buses:_Total_Estimated_Ridership" < 0
   OR "LIRR:_Total_Estimated_Ridership" < 0
   OR "Metro-North:_Total_Estimated_Ridership" < 0
   OR "Access-A-Ride:_Total_Scheduled_Trips" < 0
   OR "Bridges_and_Tunnels:_Total_Traffic" < 0
   OR "Staten_Island_Railway:_Total_Estimated_Ridership" < 0;

-- Quality Check 3:
-- Identify records with extremely high values in key numeric columns, which might be outliers.
SELECT *
FROM MTA_Daily_Ridership
WHERE "Subways:_Total_Estimated_Ridership" > 100000
   OR "Buses:_Total_Estimated_Ridership" > 100000
   OR "LIRR:_Total_Estimated_Ridership" > 100000
   OR "Metro-North:_Total_Estimated_Ridership" > 100000
   OR "Access-A-Ride:_Total_Scheduled_Trips" > 100000
   OR "Bridges_and_Tunnels:_Total_Traffic" > 100000
   OR "Staten_Island_Railway:_Total_Estimated_Ridership" > 100000;
