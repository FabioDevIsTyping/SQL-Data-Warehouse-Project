-- ==============================================
-- Validation for silver.crm_cust_info
-- ==============================================

/*
-----------------------------------------------------------------------------------------
Validation Script for Table: silver.crm_cust_info

Purpose:
This script performs post-load validation checks on the 'silver.crm_cust_info' table within the silver layer.
It verifies:
  - Key constraints (e.g., primary key nulls or duplicates)
  - Data cleanliness (e.g., whitespace issues)
  - Domain standardization (e.g., expected values)
  - Business logic correctness (e.g., cost validity, date order)

Run this after loading the silver layer to ensure data quality.
-----------------------------------------------------------------------------------------
*/

-- Test for NULLs or duplicates in primary key

SELECT cst_id, COUNT(*) FROM silver.crm_cust_info GROUP BY cst_id HAVING COUNT(*) > 1 OR cst_id IS NULL;;

-- Test for unwanted spaces

SELECT * FROM silver.crm_cust_info WHERE cst_firstname != TRIM(cst_firstname) OR cst_lastname != TRIM(cst_lastname) OR cst_gndr != TRIM(cst_gndr) OR cst_marital_status != TRIM(cst_marital_status);;

-- Check for standard values in gender and marital status

SELECT DISTINCT cst_gndr FROM silver.crm_cust_info;;

SELECT DISTINCT cst_marital_status FROM silver.crm_cust_info;;

-- ==============================================
-- Validation for silver.crm_prd_info
-- ==============================================

/*
-----------------------------------------------------------------------------------------
Validation Script for Table: silver.crm_prd_info

Purpose:
This script performs post-load validation checks on the 'silver.crm_prd_info' table within the silver layer.
It verifies:
  - Key constraints (e.g., primary key nulls or duplicates)
  - Data cleanliness (e.g., whitespace issues)
  - Domain standardization (e.g., expected values)
  - Business logic correctness (e.g., cost validity, date order)

Run this after loading the silver layer to ensure data quality.
-----------------------------------------------------------------------------------------
*/

-- Test for NULLs or duplicates in primary key

SELECT prd_id, COUNT(*) FROM silver.crm_prd_info GROUP BY prd_id HAVING COUNT(*) > 1 OR prd_id IS NULL;;

-- Test for unwanted spaces

SELECT * FROM silver.crm_prd_info WHERE TRIM(prd_nm) != prd_nm;;

-- Test for invalid costs

SELECT * FROM silver.crm_prd_info WHERE prd_cost < 0 OR prd_cost IS NULL;;

-- Check product line categories

SELECT DISTINCT prd_line FROM silver.crm_prd_info;;

-- Check start and end date logic

SELECT * FROM silver.crm_prd_info WHERE prd_start_dt > prd_end_dt;;

-- ==============================================
-- Validation for silver.crm_sales_details
-- ==============================================

/*
-----------------------------------------------------------------------------------------
Validation Script for Table: silver.crm_sales_details

Purpose:
This script performs post-load validation checks on the 'silver.crm_sales_details' table within the silver layer.
It verifies:
  - Key constraints (e.g., primary key nulls or duplicates)
  - Data cleanliness (e.g., whitespace issues)
  - Domain standardization (e.g., expected values)
  - Business logic correctness (e.g., cost validity, date order)

Run this after loading the silver layer to ensure data quality.
-----------------------------------------------------------------------------------------
*/

-- Check for NULLs in foreign keys or metrics

SELECT * FROM silver.crm_sales_details WHERE sls_ord_num IS NULL OR sls_prd_key IS NULL OR sls_cust_id IS NULL;;

-- Check for negative sales or quantities

SELECT * FROM silver.crm_sales_details WHERE sls_sales < 0 OR sls_quantity < 0 OR sls_price < 0;;

-- ==============================================
-- Validation for silver.erp_cust_az12
-- ==============================================

/*
-----------------------------------------------------------------------------------------
Validation Script for Table: silver.erp_cust_az12

Purpose:
This script performs post-load validation checks on the 'silver.erp_cust_az12' table within the silver layer.
It verifies:
  - Key constraints (e.g., primary key nulls or duplicates)
  - Data cleanliness (e.g., whitespace issues)
  - Domain standardization (e.g., expected values)
  - Business logic correctness (e.g., cost validity, date order)

Run this after loading the silver layer to ensure data quality.
-----------------------------------------------------------------------------------------
*/

-- Check for NULLs in critical fields

SELECT * FROM silver.erp_cust_az12 WHERE cid IS NULL OR bdate IS NULL OR gen IS NULL;;

-- Check for unwanted spaces

SELECT * FROM silver.erp_cust_az12 WHERE gen != TRIM(gen);;

-- ==============================================
-- Validation for silver.erp_loc_a101
-- ==============================================

/*
-----------------------------------------------------------------------------------------
Validation Script for Table: silver.erp_loc_a101

Purpose:
This script performs post-load validation checks on the 'silver.erp_loc_a101' table within the silver layer.
It verifies:
  - Key constraints (e.g., primary key nulls or duplicates)
  - Data cleanliness (e.g., whitespace issues)
  - Domain standardization (e.g., expected values)
  - Business logic correctness (e.g., cost validity, date order)

Run this after loading the silver layer to ensure data quality.
-----------------------------------------------------------------------------------------
*/

-- Check for NULLs or spaces in country field

SELECT * FROM silver.erp_loc_a101 WHERE cid IS NULL OR cntry IS NULL;;

SELECT * FROM silver.erp_loc_a101 WHERE cntry != TRIM(cntry);;

-- ==============================================
-- Validation for silver.erp_px_cat_g1v2
-- ==============================================

/*
-----------------------------------------------------------------------------------------
Validation Script for Table: silver.erp_px_cat_g1v2

Purpose:
This script performs post-load validation checks on the 'silver.erp_px_cat_g1v2' table within the silver layer.
It verifies:
  - Key constraints (e.g., primary key nulls or duplicates)
  - Data cleanliness (e.g., whitespace issues)
  - Domain standardization (e.g., expected values)
  - Business logic correctness (e.g., cost validity, date order)

Run this after loading the silver layer to ensure data quality.
-----------------------------------------------------------------------------------------
*/

-- Check for NULLs in id or mismatch with CRM category IDs

SELECT id FROM silver.erp_px_cat_g1v2 WHERE id NOT IN (SELECT cat_id FROM silver.crm_prd_info);;

-- Check for unwanted spaces

SELECT * FROM silver.erp_px_cat_g1v2 WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance);;

-- Check for standard values

SELECT DISTINCT maintenance FROM silver.erp_px_cat_g1v2;;

SELECT DISTINCT cat FROM silver.erp_px_cat_g1v2;;

SELECT DISTINCT subcat FROM silver.erp_px_cat_g1v2;;

