------------------------------------------------------------
-- SQL Data Cleaning Script for MTA Daily Ridership Data
------------------------------------------------------------
-- Table Columns:
--   Date
--   "Subways:_Total_Estimated_Ridership"
--   "Subways:_%_of_Comparable_Pre-Pandemic_Day"
--   "Buses:_Total_Estimated_Ridership"
--   "Buses:_%_of_Comparable_Pre-Pandemic_Day"
--   "LIRR:_Total_Estimated_Ridership"
--   "LIRR:_%_of_Comparable_Pre-Pandemic_Day"
--   "Metro-North:_Total_Estimated_Ridership"
--   "Metro-North:_%_of_Comparable_Pre-Pandemic_Day"
--   "Access-A-Ride:_Total_Scheduled_Trips"
--   "Access-A-Ride:_%_of_Comparable_Pre-Pandemic_Day"
--   "Bridges_and_Tunnels:_Total_Traffic"
--   "Bridges_and_Tunnels:_%_of_Comparable_Pre-Pandemic_Day"
--   "Staten_Island_Railway:_Total_Estimated_Ridership"
--   "Staten_Island_Railway:_%_of_Comparable_Pre-Pandemic_Day"

------------------------------------------------------------
-- Section 1: Understand Your Data
------------------------------------------------------------
-- Preview the first 5 rows to inspect the structure and content.
SELECT *
FROM MTA_Daily_Ridership
LIMIT 5;

------------------------------------------------------------
-- Section 2: Data Conversion & Standardization
------------------------------------------------------------
-- 2.1: Standardize the Date column to ISO 8601 (YYYY-MM-DD) format.
-- This assumes that Date values are stored in a recognizable format.
UPDATE MTA_Daily_Ridership
SET Date = strftime('%Y-%m-%d', Date)
WHERE Date IS NOT NULL;

-- 2.2: Convert ridership columns from text to INTEGER if needed.
-- These updates convert values and handle potential empty strings.
UPDATE MTA_Daily_Ridership
SET "Subways:_Total_Estimated_Ridership" = CAST("Subways:_Total_Estimated_Ridership" AS INTEGER)
WHERE "Subways:_Total_Estimated_Ridership" IS NOT NULL AND "Subways:_Total_Estimated_Ridership" <> '';

UPDATE MTA_Daily_Ridership
SET "Buses:_Total_Estimated_Ridership" = CAST("Buses:_Total_Estimated_Ridership" AS INTEGER)
WHERE "Buses:_Total_Estimated_Ridership" IS NOT NULL AND "Buses:_Total_Estimated_Ridership" <> '';

UPDATE MTA_Daily_Ridership
SET "LIRR:_Total_Estimated_Ridership" = CAST("LIRR:_Total_Estimated_Ridership" AS INTEGER)
WHERE "LIRR:_Total_Estimated_Ridership" IS NOT NULL AND "LIRR:_Total_Estimated_Ridership" <> '';

UPDATE MTA_Daily_Ridership
SET "Metro-North:_Total_Estimated_Ridership" = CAST("Metro-North:_Total_Estimated_Ridership" AS INTEGER)
WHERE "Metro-North:_Total_Estimated_Ridership" IS NOT NULL AND "Metro-North:_Total_Estimated_Ridership" <> '';

UPDATE MTA_Daily_Ridership
SET "Access-A-Ride:_Total_Scheduled_Trips" = CAST("Access-A-Ride:_Total_Scheduled_Trips" AS INTEGER)
WHERE "Access-A-Ride:_Total_Scheduled_Trips" IS NOT NULL AND "Access-A-Ride:_Total_Scheduled_Trips" <> '';

UPDATE MTA_Daily_Ridership
SET "Bridges_and_Tunnels:_Total_Traffic" = CAST("Bridges_and_Tunnels:_Total_Traffic" AS INTEGER)
WHERE "Bridges_and_Tunnels:_Total_Traffic" IS NOT NULL AND "Bridges_and_Tunnels:_Total_Traffic" <> '';

UPDATE MTA_Daily_Ridership
SET "Staten_Island_Railway:_Total_Estimated_Ridership" = CAST("Staten_Island_Railway:_Total_Estimated_Ridership" AS INTEGER)
WHERE "Staten_Island_Railway:_Total_Estimated_Ridership" IS NOT NULL AND "Staten_Island_Railway:_Total_Estimated_Ridership" <> '';

------------------------------------------------------------
-- Section 3: Handling Duplicates
------------------------------------------------------------
-- Check for duplicate records based on the Date column (each date should be unique).
SELECT 
    Date,
    COUNT(*) AS duplicate_count
FROM MTA_Daily_Ridership
GROUP BY Date
HAVING COUNT(*) > 1;

-- Remove duplicates by creating a new table with only distinct rows.
CREATE TABLE MTA_Daily_Ridership_Clean AS
SELECT DISTINCT *
FROM MTA_Daily_Ridership;

------------------------------------------------------------
-- Section 4: Handling Missing Values or Blanks
------------------------------------------------------------
-- Identify rows with missing or blank values in key columns.
SELECT *
FROM MTA_Daily_Ridership
WHERE Date IS NULL OR Date = ''
   OR "Subways:_Total_Estimated_Ridership" IS NULL OR "Subways:_Total_Estimated_Ridership" = ''
   OR "Buses:_Total_Estimated_Ridership" IS NULL OR "Buses:_Total_Estimated_Ridership" = '';

-- Option 1: Remove rows with missing or blank values.
DELETE FROM MTA_Daily_Ridership
WHERE Date IS NULL OR Date = ''
   OR "Subways:_Total_Estimated_Ridership" IS NULL OR "Subways:_Total_Estimated_Ridership" = ''
   OR "Buses:_Total_Estimated_Ridership" IS NULL OR "Buses:_Total_Estimated_Ridership" = '';

-- Option 2: Alternatively, update missing or blank values with default values.
-- For example, set missing ridership numbers to 0.
UPDATE MTA_Daily_Ridership
SET "Subways:_Total_Estimated_Ridership" = 0
WHERE "Subways:_Total_Estimated_Ridership" IS NULL OR "Subways:_Total_Estimated_Ridership" = '';

-- Additional UPDATE statements can be added for other columns as needed.
------------------------------------------------------------
-- End of Data Cleaning Script for MTA Daily Ridership Data
------------------------------------------------------------
