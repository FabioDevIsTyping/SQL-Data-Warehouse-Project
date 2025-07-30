/*
---------------------------------------------------------------------------------------------------
 View Name: gold.dim_customers

 Purpose:
 This view creates a customer dimension for the gold layer by combining customer information 
 from both CRM and ERP sources. It consolidates keys, names, demographic data, and origin country 
 into a clean dimensional format suitable for analytical querying.

 Key Features:
   - Generates a surrogate key (`customer_key`) using ROW_NUMBER
   - Merges CRM and ERP customer data on customer key (`cst_key` ↔ `cid`)
   - Prioritizes gender from CRM, but falls back on ERP if unknown
   - Provides standardized field names for reporting use

 Source Tables:
   - silver.crm_cust_info (CRM customer master)
   - silver.erp_cust_az12 (ERP customer demographics)
   - silver.erp_loc_a101 (ERP customer location)
---------------------------------------------------------------------------------------------------
*/

CREATE VIEW gold.dim_customers AS 
SELECT 
    -- Surrogate key for the dimension
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,

    -- Core identifiers and names
    ci.cst_id           AS customer_id,
    ci.cst_key          AS customer_number,
    ci.cst_firstname    AS first_name,
    ci.cst_lastname     AS last_name,

    -- Country of origin (from ERP location table)
    la.cntry            AS country,

    -- Marital status from CRM
    ci.cst_marital_status AS marital_status,

    -- Gender: prefer CRM value unless it's 'Unknown', then use ERP value
    CASE 
        WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen, 'Unknown')
    END AS gender,

    -- Birthdate from ERP
    ca.bdate            AS birthdate,

    -- Date when customer record was created
    ci.cst_create_date  AS create_date

FROM silver.crm_cust_info AS ci

-- Join with ERP customer demographic data
LEFT JOIN silver.erp_cust_az12 AS ca
    ON ci.cst_key = ca.cid

-- Join with ERP customer location data
LEFT JOIN silver.erp_loc_a101 AS la
    ON ci.cst_key = la.cid;
