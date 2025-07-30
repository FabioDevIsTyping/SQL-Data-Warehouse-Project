/*
---------------------------------------------------------------------------------------------
 Script Name: silver_transform_erp_cust_az12.sql

 Purpose:
 This script performs data quality checks and transformations on customer demographic 
 data from the ERP system. It loads the cleaned and standardized data from the bronze 
 layer to the silver layer.

 Key Transformations:
   - Removes invalid birth dates (future dates)
   - Standardizes gender values (e.g., 'F', 'FEMALE' → 'Female')
   - Removes the 'NAS' prefix from customer IDs when present
   - Truncates the target table before reloading (full load)

 Assumptions:
   - Source table: bronze.erp_cust_az12
   - Target table: silver.erp_cust_az12
   - `bdate` is a DATE column
   - `cid` may start with 'NAS' and needs normalization
   - `gen` may be in various cases/forms and needs unification
---------------------------------------------------------------------------------------------
*/

-- ========================================================================================
-- STEP 1: Data Inspection on Bronze Table
-- ========================================================================================

-- View raw data
SELECT *
FROM bronze.erp_cust_az12;

-- Identify invalid birth dates (too old or in the future)
SELECT DISTINCT bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE();

-- Explore distinct gender values (for standardization)
SELECT DISTINCT gen 
FROM bronze.erp_cust_az12;

-- ========================================================================================
-- STEP 2: Load Procedure for Silver Table
-- ========================================================================================

CREATE OR ALTER PROCEDURE silver.load_erp_cust_az12 AS 
BEGIN
    -- Clear out existing records to allow full refresh
    TRUNCATE TABLE silver.erp_cust_az12; 

    -- Insert cleaned and standardized records into silver layer
    INSERT INTO silver.erp_cust_az12 (
        cid, 
        bdate,
        gen
    )
    SELECT
        -- Normalize customer ID by removing 'NAS' prefix if present
        CASE 
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
            ELSE cid
        END AS cid,

        -- Nullify invalid future birthdates
        CASE 
            WHEN bdate > GETDATE() THEN NULL 
            ELSE bdate
        END AS bdate,

        -- Standardize gender values
        CASE 
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'Unknown'
        END AS gen
    FROM bronze.erp_cust_az12;
END;

-- ========================================================================================
-- STEP 3: Post-Load Validation on Silver Table
-- ========================================================================================

-- Verify the loaded records
SELECT *
FROM silver.erp_cust_az12;

-- Re-check for invalid birthdates after transformation
SELECT DISTINCT bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE();

-- Re-check standardized gender values
SELECT DISTINCT gen 
FROM silver.erp_cust_az12;
