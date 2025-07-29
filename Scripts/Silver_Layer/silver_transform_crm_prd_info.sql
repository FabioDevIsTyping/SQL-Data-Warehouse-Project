/*
---------------------------------------------------------------------------------------------------
 Script Name: silver_transform_crm_prd_info.sql

 Purpose:
 This script validates and transforms product data from the bronze layer to the silver layer.
 It performs data quality checks on primary keys, names, costs, and dates, and standardizes
 product line values and derives a category ID from the product key.

 Transformations include:
   - Trimming and checking for nulls or invalid values
   - Standardizing `prd_line` values
   - Deriving a `cat_id` from the first part of the `prd_key`
   - Calculating `prd_end_dt` as one day before the next product's start date (windowed logic)

 Assumptions:
   - Source table: bronze.crm_prd_info
   - Target table: silver.crm_prd_info
   - prd_id is the primary key
   - prd_key is of format 'CAT01-XXXXX' or similar

---------------------------------------------------------------------------------------------------
*/

-- ====================================================================================
-- STEP 1: DATA QUALITY CHECKS ON BRONZE TABLE
-- ====================================================================================

-- Check for NULLs or duplicate primary keys
SELECT 
    prd_id,
    COUNT(*) AS cnt
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check for unwanted spaces in product name
SELECT prd_nm 
FROM bronze.crm_prd_info
WHERE TRIM(prd_nm) != prd_nm;

-- Check for null, negative, or zero product costs
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Review distinct values for product line classification
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info;

-- Check for logical inconsistencies in dates
SELECT * 
FROM bronze.crm_prd_info 
WHERE prd_start_dt > prd_end_dt;

-- ====================================================================================
-- STEP 2: TRANSFORMATION INTO SILVER TABLE
-- ====================================================================================

CREATE OR ALTER PROCEDURE silver.load_crm_prd_info AS 
BEGIN
    SET NOCOUNT ON;

    PRINT 'Truncating silver.crm_prd_info...';
    TRUNCATE TABLE silver.crm_prd_info;

    PRINT 'Loading silver.crm_prd_info...';
    INSERT INTO silver.crm_prd_info (
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )
    SELECT 
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,  -- Derive category ID from prefix
        SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,          -- Remove prefix from product key
        TRIM(prd_nm) AS prd_nm,                                 -- Clean name
        ISNULL(prd_cost, 0) AS prd_cost,                        -- Default cost if NULL

        -- Standardize product line descriptions
        CASE UPPER(TRIM(prd_line))
            WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'other Sales'
            WHEN 'T' THEN 'Touring'
            ELSE 'Unknown' 
        END AS prd_line,

        -- Ensure dates are cast to DATE only
        CAST(prd_start_dt AS DATE) AS prd_start_dt,

        -- Compute prd_end_dt as one day before the next product's start date
        CAST(
            DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (
                PARTITION BY prd_key 
                ORDER BY prd_start_dt ASC
            )) AS DATE
        ) AS prd_end_dt

    FROM bronze.crm_prd_info;
END;

-- ====================================================================================
-- STEP 3: VALIDATION ON SILVER TABLE
-- ====================================================================================

-- Check for NULLs or duplicate keys
SELECT 
    prd_id,
    COUNT(*) AS cnt
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check for remaining spaces
SELECT prd_nm 
FROM silver.crm_prd_info
WHERE TRIM(prd_nm) != prd_nm;

-- Check for invalid product costs
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Review standardized product line values
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;

-- Validate start/end date consistency
SELECT * 
FROM silver.crm_prd_info 
WHERE prd_start_dt > prd_end_dt;
