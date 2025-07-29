/*
---------------------------------------------------------------------------------------------------
 Script Name: silver_transform_crm_cust_info.sql

 Purpose:
 This script performs the transformation of customer data from the bronze layer to the silver layer.
 It applies standardization, formatting, and de-duplication logic based on the most recent record
 per customer (`cst_id`). The script includes data quality checks before and after transformation.

 Key Transformations:
   - Removes unwanted whitespace from text fields
   - Standardizes gender and marital status values
   - Filters out records with NULL `cst_id`
   - Keeps only the most recent record for each customer based on `create_date`
   - Truncates the `silver.crm_cust_info` table before inserting fresh data

 Assumptions:
   - The source table is `bronze.crm_cust_info`
   - The target table is `silver.crm_cust_info`
   - `cst_id` is the primary key in the silver layer

---------------------------------------------------------------------------------------------------
*/

-- STEP 1: DATA QUALITY CHECKS ON BRONZE TABLE (BEFORE TRANSFORMATION)

-- Check for NULLs or duplicate keys
SELECT 
    cst_id,
    COUNT(*) AS cnt
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check for unwanted leading/trailing spaces
SELECT cst_firstname FROM bronze.crm_cust_info WHERE cst_firstname != TRIM(cst_firstname);
SELECT cst_lastname   FROM bronze.crm_cust_info WHERE cst_lastname   != TRIM(cst_lastname);
SELECT cst_gndr       FROM bronze.crm_cust_info WHERE cst_gndr       != TRIM(cst_gndr);
SELECT cst_marital_status FROM bronze.crm_cust_info WHERE cst_marital_status != TRIM(cst_marital_status);

-- Review distinct values for gender and marital status
SELECT DISTINCT cst_gndr FROM bronze.crm_cust_info;
SELECT DISTINCT cst_marital_status FROM bronze.crm_cust_info;

-- STEP 2: TRANSFORMATION INTO SILVER TABLE
CREATE OR ALTER PROCEDURE silver.load_crm_cust_info AS 
BEGIN 
    SET NOCOUNT ON;
    PRINT('Starting silver.crm_cust_info load ...');
    -- Clear existing records in the silver layer before inserting
    TRUNCATE TABLE silver.crm_cust_info;

    -- Insert transformed and cleaned data
    INSERT INTO silver.crm_cust_info (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date
    )
    SELECT 
        cst_id,
        cst_key,
        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname) AS cst_lastname,
        CASE 
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'Unknown'
        END AS cst_marital_status,
        CASE 
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'Unknown'
        END AS cst_gndr,
        create_date
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY create_date DESC) AS flag_last
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    ) t
    WHERE flag_last = 1;
END;

-- STEP 3: VALIDATION ON SILVER TABLE (AFTER TRANSFORMATION)

-- Check for NULLs or duplicates in the primary key
SELECT 
    cst_id,
    COUNT(*) AS cnt
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Recheck for unwanted whitespace
SELECT cst_firstname FROM silver.crm_cust_info WHERE cst_firstname != TRIM(cst_firstname);
SELECT cst_lastname   FROM silver.crm_cust_info WHERE cst_lastname   != TRIM(cst_lastname);
SELECT cst_gndr       FROM silver.crm_cust_info WHERE cst_gndr       != TRIM(cst_gndr);
SELECT cst_marital_status FROM silver.crm_cust_info WHERE cst_marital_status != TRIM(cst_marital_status);

-- Explicit NULL check on key
SELECT * 
FROM silver.crm_cust_info 
WHERE cst_id IS NULL;
