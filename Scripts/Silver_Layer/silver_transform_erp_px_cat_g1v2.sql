/*
---------------------------------------------------------------------------------------------------
 Script Name: silver_transform_erp_px_cat_g1v2.sql

 Purpose:
 This script validates and transforms the ERP product category data from the bronze layer to 
 the silver layer. It performs checks for data quality (e.g., unwanted spaces, nulls, standardization)
 and transfers clean data into the `silver.erp_px_cat_g1v2` table.

 Key Checks and Transformations:
   - Verifies ID consistency with `silver.crm_prd_info.cat_id`
   - Detects leading/trailing whitespace in string columns
   - Highlights distinct values for `maintenance`, `cat`, and `subcat`
   - Clears old silver table data before inserting new records

 Assumptions:
   - Source table: bronze.erp_px_cat_g1v2
   - Target table: silver.erp_px_cat_g1v2
   - Column `id` should match `cat_id` in `silver.crm_prd_info`

---------------------------------------------------------------------------------------------------
*/

-- ====================================================================================
-- STEP 1: QUALITY CHECKS ON BRONZE DATA
-- ====================================================================================

-- IDs in bronze not matching silver.crm_prd_info.cat_id (useful for join integrity)
SELECT id
FROM bronze.erp_px_cat_g1v2
WHERE id NOT IN (
    SELECT cat_id FROM silver.crm_prd_info
);

-- Check for unwanted leading/trailing spaces in all string fields
SELECT cat, subcat, maintenance
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) 
   OR subcat != TRIM(subcat) 
   OR maintenance != TRIM(maintenance);

-- Explore unique values for standardization (e.g., inconsistent capitalization)
SELECT DISTINCT maintenance FROM bronze.erp_px_cat_g1v2;
SELECT DISTINCT cat        FROM bronze.erp_px_cat_g1v2;
SELECT DISTINCT subcat     FROM bronze.erp_px_cat_g1v2;

-- ====================================================================================
-- STEP 2: TRANSFORM AND LOAD INTO SILVER
-- ====================================================================================

CREATE OR ALTER PROCEDURE silver.load_erp_px_cat_g1v2 AS
BEGIN
    SET NOCOUNT ON;

    PRINT 'Truncating and loading silver.erp_px_cat_g1v2...';

    TRUNCATE TABLE silver.erp_px_cat_g1v2;

    INSERT INTO silver.erp_px_cat_g1v2 (
        id,
        cat,
        subcat,
        maintenance
    )
    SELECT 
        TRIM(id),
        TRIM(cat),
        TRIM(subcat),
        TRIM(maintenance)
    FROM bronze.erp_px_cat_g1v2;
END;

-- ====================================================================================
-- STEP 3: VALIDATION AFTER INSERT
-- ====================================================================================

-- Check ID consistency again (now in silver)
SELECT id
FROM silver.erp_px_cat_g1v2
WHERE id NOT IN (
    SELECT cat_id FROM silver.crm_prd_info
);

-- Re-check for any remaining spaces
SELECT cat, subcat, maintenance
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat)
   OR subcat != TRIM(subcat)
   OR maintenance != TRIM(maintenance);

-- Check distinct values for normalization opportunities
SELECT DISTINCT maintenance FROM silver.erp_px_cat_g1v2;
SELECT DISTINCT cat        FROM silver.erp_px_cat_g1v2;
SELECT DISTINCT subcat     FROM silver.erp_px_cat_g1v2;
