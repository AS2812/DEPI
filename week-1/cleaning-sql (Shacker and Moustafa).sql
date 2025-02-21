------------------------------------------------------------
-- SQL Data Cleaning Script for MTA Daily Ridership Data
------------------------------------------------------------
-- Table Columns:
--   Date
--   Subways:_Total_Estimated_Ridership
--   Subways:_%_of_Comparable_Pre-Pandemic_Day
--   Buses:_Total_Estimated_Ridership
--   Buses:_%_of_Comparable_Pre-Pandemic_Day
--   LIRR:_Total_Estimated_Ridership
--   LIRR:_%_of_Comparable_Pre-Pandemic_Day
--   Metro-North:_Total_Estimated_Ridership
--   Metro-North:_%_of_Comparable_Pre-Pandemic_Day
--   Access-A-Ride:_Total_Scheduled_Trips
--   Access-A-Ride:_%_of_Comparable_Pre-Pandemic_Day
--   Bridges_and_Tunnels:_Total_Traffic
--   Bridges_and_Tunnels:_%_of_Comparable_Pre-Pandemic_Day
--   Staten_Island_Railway:_Total_Estimated_Ridership
--   Staten_Island_Railway:_%_of_Comparable_Pre-Pandemic_Day

------------------------------------------------------------
-- Section 1: Understand Your Data
------------------------------------------------------------
-- Preview the first 5 rows to check the dataset structure.
SELECT *
FROM MTA_Daily_Ridership
LIMIT 5;

------------------------------------------------------------
-- Section 2: Handling Duplicates
------------------------------------------------------------
-- Check for duplicate records based on the Date column.
-- We assume each date should be unique.
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
-- Section 3: Handling Missing Values or Blanks
------------------------------------------------------------
-- Identify rows with missing (NULL) or blank values in key columns.
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

-- Option 2: Alternatively, update missing or blank ridership values to default values.
-- For example, if a ridership value is missing, set it to 0.
UPDATE MTA_Daily_Ridership
SET "Subways:_Total_Estimated_Ridership" = 0
WHERE "Subways:_Total_Estimated_Ridership" IS NULL OR "Subways:_Total_Estimated_Ridership" = '';

-- You can add similar UPDATE statements for other columns as needed.
------------------------------------------------------------
-- End of Data Cleaning Script for MTA Daily Ridership Data
------------------------------------------------------------
