/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-----------------------------------------------------------------------
-- create dimension table : gold.dim_customers
-----------------------------------------------------------------------
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers as (
SELECT 
ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
ci.cst_id as customer_id,
ci.cst_key as customer_number,
ci.cst_firstname as first_name,
ci.cst_lastname as last_name,
ci.cst_marital_status as marital_status,
CASE WHEN ci.cst_gndr != 'n/a' then ci.cst_gndr
	ELSE COALESCE(ca.gen,'n/a')
END AS gender,
la.cntry as country,
ca.bdate as birthdate,
ci.cst_create_date as create_date
from silver.crm_cust_info ci left join silver.erp_cust_az12 ca on ci.cst_key = ca.cid
left join silver.erp_loc_a101 la on ci.cst_key = la.cid
)
;
GO
-----------------------------------------------------------------------
-- create dimension table : gold.dim_products
-----------------------------------------------------------------------
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

create view gold.dim_products as (
select 
ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt,pn.prd_key) AS product_key,
pn.prd_id as product_id,
pn.prd_key as product_number,
pn.prd_nm as product_name,
pn.prd_line as product_line,
pn.cat_id as category_id,
px.cat as category,
px.subcat as subcategory,
px.maintenance,
pn.prd_cost as cost,
pn.prd_start_dt as start_date
from silver.crm_prd_info pn left join silver.erp_px_cat_g1v2  px on pn.cat_id = px.id 
where prd_end_dt IS NULL --Filters the historical data
)
;
GO

-----------------------------------------------------------------------
-- create fact table : gold.fact_sales
-----------------------------------------------------------------------

IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

create view gold.fact_sales as (
select 
sd.sls_ord_num as order_number,
pr.product_key,
cu.customer_key,
sls_order_dt as order_date,
sls_ship_dt as shipping_date,
sls_due_dt as due_date,
sls_sales as sales_amount,
sls_quantity as quantity,
sls_price as price
from silver.crm_sales_details sd left join gold.dim_products pr on sd.sls_prd_key = pr.product_number
left join gold.dim_customers cu on sd.sls_cust_id = cu.customer_id
);
GO
