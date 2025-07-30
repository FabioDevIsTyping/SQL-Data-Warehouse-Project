/*
---------------------------------------------------------------------------------------
Script Name: gold.fact_sales.sql

Purpose:
This script defines the `gold.fact_sales` view, which represents the **sales fact table**
in the dimensional model. It joins cleaned transactional data from the silver layer
with corresponding dimension tables to produce a well-structured star schema.

The fact table includes:
  - Surrogate keys to link with product and customer dimensions
  - Transactional metrics: order date, shipping date, due date
  - Measures: sales amount, quantity sold, and unit price

Assumptions:
  - `silver.crm_sales_details` contains transactional sales data
  - `gold.dim_products` and `gold.dim_customers` expose surrogate keys for joining
  - Business keys `sls_prd_key` and `sls_cust_id` match the `product_number` and `customer_id`

---------------------------------------------------------------------------------------
*/

CREATE VIEW gold.fact_sales AS
SELECT
	sd.sls_ord_num AS order_number,             -- Original sales order number
	pr.product_key,                             -- Surrogate key from dim_products
	cu.customer_key,                            -- Surrogate key from dim_customers
	sd.sls_order_dt AS order_date,              -- Date when the order was placed
	sd.sls_ship_dt AS shipping_date,            -- Date when the order was shipped
	sd.sls_due_dt AS due_date,                  -- Promised delivery date
	sd.sls_sales AS sales_amount,               -- Total sales amount
	sd.sls_quantity AS quantity,                -- Number of units sold
	sd.sls_price AS price                       -- Price per unit
FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_products AS pr
	ON sd.sls_prd_key = pr.product_number       -- Join on business key (product)
LEFT JOIN gold.dim_customers AS cu 
	ON sd.sls_cust_id = cu.customer_id;         -- Join on business key (customer)
