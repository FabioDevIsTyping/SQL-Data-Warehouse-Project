/*
---------------------------------------------------------------------------------------------------
 Script Name: silver_transform_crm_sales_details.sql

 Purpose:
 This script validates and transforms sales transaction data from the bronze layer to the silver
 layer. It includes data quality checks for invalid dates, incorrect sales calculations, and 
 missing or inconsistent values. It also ensures dates are converted properly and fixes incorrect
 `sls_sales` and `sls_price` values when needed.

 Key Transformations:
   - Converts 8-digit integer dates into proper DATE format
   - Sets invalid date fields to NULL
   - Fixes sls_sales when inconsistent with quantity and price
   - Calculates sls_price when missing or invalid
   - Excludes sales records for customers already present in silver.crm_cust_info

 Assumptions:
   - Source table: bronze.crm_sales_details
   - Target table: silver.crm_sales_details
   - Dates are stored as 8-digit integers (e.g. 20250101)

---------------------------------------------------------------------------------------------------
*/

-- ====================================================================================
-- STEP 1: DATA QUALITY CHECKS ON BRONZE TABLE
-- ====================================================================================

-- Check for null, zero, or incorrectly formatted order dates
SELECT sls_order_dt 
FROM bronze.crm_sales_details
WHERE sls_order_dt IS NULL OR sls_order_dt <= 0 OR LEN(sls_order_dt) != 8;

-- Check for logical inconsistencies in date relationships
SELECT * 
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

-- Check for sales calculation inconsistencies
SELECT
    sls_sales,
    sls_quantity,
    sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
   OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0;

-- ====================================================================================
-- STEP 2: TRANSFORMATION INTO SILVER TABLE
-- ====================================================================================

CREATE OR ALTER PROCEDURE silver.load_crm_sales_details AS 
BEGIN 
    SET NOCOUNT ON;

    PRINT('Truncating silver.crm_sales_details...');
    TRUNCATE TABLE silver.crm_sales_details;

    PRINT('Loading silver.crm_sales_details...');
    INSERT INTO silver.crm_sales_details (
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
    )
    SELECT 
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,

        -- Convert sls_order_dt from INT (yyyymmdd) to DATE, or set to NULL if invalid
        CASE 
            WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
            ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
        END AS sls_order_dt,

        -- Same logic for ship date
        CASE 
            WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
            ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
        END AS sls_ship_dt,

        -- Same logic for due date
        CASE 
            WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
            ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
        END AS sls_due_dt,

        -- Fix sls_sales if NULL, zero, or inconsistent with quantity * price
        CASE 
            WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END AS sls_sales,

        -- Keep quantity as-is (expected to be reliable)
        sls_quantity,

        -- Fix sls_price if NULL or non-positive
        CASE 
            WHEN sls_price IS NULL OR sls_price <= 0 
                THEN sls_sales / NULLIF(sls_quantity, 0)
            ELSE sls_price
        END AS sls_price

    FROM bronze.crm_sales_details;
    PRINT('silver.crm_sales_details load completed.');
END;

-- ====================================================================================
-- STEP 3: VALIDATION ON SILVER TABLE
-- ====================================================================================

-- Check for invalid date relationships
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

-- Check for incorrect sales/price/quantity relationships
SELECT
    sls_sales,
    sls_quantity,
    sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
   OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0;

SELECT *
FROM silver.crm_sales_details

