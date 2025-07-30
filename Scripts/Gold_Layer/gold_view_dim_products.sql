/*
=======================================================================================
 View Name: gold.dim_products

 Purpose:
 This view creates a product dimension table for the gold layer of the data warehouse.
 It integrates data from the silver.crm_prd_info (product master) and 
 silver.erp_px_cat_g1v2 (product category) tables.

 Key Features:
   - Assigns a unique surrogate key (`product_key`) using ROW_NUMBER.
   - Joins product and category data using `cat_id` = `id`.
   - Filters only the most recent version of each product (`prd_end_dt IS NULL`).
   - Provides cleaned and categorized product data ready for analytical consumption.

 Output Columns:
   - product_key: surrogate key for the dimension table
   - product_id: original product ID
   - product_number: parsed product key (from bronze transformation)
   - product_name: trimmed product name
   - category_id: derived from prefix in `prd_key`
   - category, subcategory, maintenance: from erp category reference
   - cost: cleaned product cost
   - product_line: standardized line (e.g., Mountain, Road)
   - start_date: effective date for the product version

 Assumptions:
   - The `prd_key` has been cleaned and transformed in the silver layer.
   - Only current (active) product records are required (prd_end_dt IS NULL).
=======================================================================================
*/

CREATE VIEW gold.dim_products AS
SELECT
	-- Generate a surrogate key for the dimension
	ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,

	-- Original product ID and cleaned key
	pn.prd_id           AS product_id,
	pn.prd_key          AS product_number,
	pn.prd_nm           AS product_name,

	-- Category relationships
	pn.cat_id           AS category_id,
	pc.cat              AS category,
	pc.subcat           AS subcategory,
	pc.maintenance      AS maintenance,

	-- Product attributes
	pn.prd_cost         AS cost,
	pn.prd_line         AS product_line,
	pn.prd_start_dt     AS start_date

FROM silver.crm_prd_info AS pn
LEFT JOIN silver.erp_px_cat_g1v2 AS pc
	ON pn.cat_id = pc.id

-- Include only current records (historical ones have a non-null prd_end_dt)
WHERE pn.prd_end_dt IS NULL;
