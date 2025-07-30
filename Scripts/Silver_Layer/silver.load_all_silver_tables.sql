/* Script:
Complete procedure to load all the silver layer tables, references the other procedures done individually.
*/


-- Central runner procedure:
CREATE OR ALTER PROCEDURE silver.load_all_silver_tables AS
BEGIN
    EXEC silver.load_crm_cust_info;
    EXEC silver.load_crm_sales_details;
    EXEC silver.load_crm_prd_info;

    EXEC silver.load_erp_cust_az12;
    EXEC silver.load_erp_loc_a101;
    EXEC silver.load_erp_px_cat_g1v2;
END;

-- Execution example:
EXEC silver.load_all_silver_tables;
