/*
---------------------------------------------------------------------------------------------
 Script Name: silver_transform_erp_loc_a101.sql

 Purpose:
 This script processes location records from the ERP system stored in the `bronze` layer,
 cleans and normalizes the data, and loads it into the `silver` layer for downstream use.

 Key Transformations:
   - Removes hyphens from customer IDs (`cid`)
   - Standardizes country codes to country names:
       'DE' → 'Germany'
       'US' or 'USA' → 'United States'
       NULL or empty → 'Unknown'
   - Truncates the silver table before insertion (full refresh logic)

 Assumptions:
   - Source table: bronze.erp_loc_a101
   - Target table: silver.erp_loc_a101
   - `cid` may contain hyphens
   - `cntry` may be null, empty, or encoded using ISO country codes
---------------------------------------------------------------------------------------------
*/

-- ========================================================================================
-- STEP 1: Explore Raw Data from Bronze Layer
-- ========================================================================================

-- View all source records
SELECT * 
FROM bronze.erp_loc_a101;

-- Inspect distinct values for country column
SELECT DISTINCT cntry
FROM bronze.erp_loc_a101
ORDER BY cntry;

-- ========================================================================================
-- STEP 2: Define Stored Procedure to Load Silver Layer
-- ========================================================================================

CREATE OR ALTER PROCEDURE silver.load_erp_loc_a101 AS
BEGIN
    -- Clear any existing records (full load strategy)
	TRUNCATE TABLE silver.erp_loc_a101;

	-- Insert cleaned data into the silver layer
	INSERT INTO silver.erp_loc_a101 (
	    cid,
	    cntry
	)
	SELECT 
	    -- Remove hyphens from customer ID
	    REPLACE(cid, '-', '') AS cid,

	    -- Normalize country names
	    CASE 
	        WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
	        WHEN UPPER(TRIM(cntry)) IN ('US', 'USA') THEN 'United States'
	        WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'Unknown'
	        ELSE TRIM(cntry)
	    END AS cntry
	FROM bronze.erp_loc_a101;
END;

-- ========================================================================================
-- STEP 3: Validate Loaded Data in Silver Layer
-- ========================================================================================

-- View transformed records
SELECT * 
FROM silver.erp_loc_a101;

-- Check distinct standardized country names
SELECT DISTINCT cntry
FROM silver.erp_loc_a101
ORDER BY cntry;
