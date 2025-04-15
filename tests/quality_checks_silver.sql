/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/


--Quality checks in Silver table.

-- ====================================================================
-- Checking 'silver.crm_cust_info'
-- ====================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
SELECT 
    cst_id,
    COUNT(*) 
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT 
    cst_key 
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key);

-- Data Standardization & Consistency
SELECT DISTINCT 
    cst_marital_status 
FROM silver.crm_cust_info;

--To find the duplicates and NULLs in id column
SELECT cst_id,
count(*) as tot_cnt
from silver.crm_cust_info
where cst_id is not null
group by cst_id
having count(*) > 1

--To find whether we are having spaces in columns

SELECT cst_firstname
from silver.crm_cust_info
where cst_firstname != TRIM(cst_firstname) ;--this will find if the firstname with spaces and after trimming are same or not.if it is not same then we can determine that we have spaces.


--To find whether we are having spaces in columns

SELECT cst_lastname
from silver.crm_cust_info
where cst_lastname != TRIM(cst_lastname) ;

-- To find the distinct values for data standardization & consistency.
select distinct cst_gndr from silver.crm_cust_info;

select distinct cst_marital_status from silver.crm_cust_info;
===========================================================================
Quality check: silver.crm_prd_info
============================================================================
--To find the prd_id has any NULLs:
SELECT prd_id,
count(*) as tot_cnt
from silver.crm_prd_info
where prd_id is null
group by prd_id
--having count(*) > 1

--To get the prd key according to erp table:
select * from silver.erp_px_cat_g1v2;
SELECT REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id from silver.crm_prd_info;
SELECT SUBSTRING(prd_key,7,LEN(prd_key)) as prd_key from silver.crm_prd_info;

--To find any unwanted spaces are present:
select prd_nm from  silver.crm_prd_info where prd_nm != TRIM(prd_nm);

--To find the prd_cost is negative or null: 

select prd_cost from silver.crm_prd_info where prd_cost is null or prd_cost < 0;

-- to find the distinct values for low cardinality columns:

SELECT distinct prd_line from silver.crm_prd_info;

-- To maintain data standardization & consistency:
SELECT CASE UPPER(TRIM(prd_line)) 
	WHEN 'M' THEN 'Mountain'
	WHEN 'R' THEN 'Road'
	WHEN 'S' THEN 'Other Sales'
	WHEN 'T' THEN 'Touring'
END as prd_line FROM silver.crm_prd_info;

--To eliminate data issue for start and end dates:

select prd_key,prd_start_dt,prd_end_dt from silver.crm_prd_info where prd_start_dt > prd_end_dt;

select prd_key ,prd_start_dt,prd_end_dt,
DATEADD(day,-1,LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) as prd_end_test 
from  silver.crm_prd_info;

===========================================================================
Quality check: silver.crm_sales_details
============================================================================

-- quality check in sales table:

select sls_ord_num from silver.crm_sales_details where sls_ord_num != trim(sls_ord_num);
select sls_prd_key from silver.crm_sales_details where sls_prd_key != trim(sls_prd_key);

select sls_prd_key from 
silver.crm_sales_details where sls_prd_key NOT IN (
select prd_key from silver.crm_prd_info);

select sls_cust_id from 
silver.crm_sales_details where sls_cust_id NOT IN (
select cst_id from silver.crm_cust_info);

select sls_order_dt,NULLIF(sls_order_dt,0) from silver.crm_sales_details 
where sls_order_dt <=0  --NEGATIVE VALUES OR 0 SHOULD NOT BE PRESENT FOR DATES. 
OR LEN(sls_order_dt)!=8 --TO CHECK IF THE DATE IN INT FORMAT HAS DDMMYYYY CHARACTERS
OR sls_order_dt > 20500101 --TO CHECK THE MAX DATE
OR sls_order_dt < 19000101; --TO CHECK THE MIN DATE

--To check if the order date is larger than the shipping date and due date:
select * from silver.crm_sales_details  where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt;

--sales must be equal to quantity * price. also all three cols are positive numbers.

select sls_ord_num,sls_sales,
sls_quantity,
CASE WHEN sls_sales <=0 OR sls_sales IS NULL OR sls_sales != (sls_quantity * ABS(sls_price)) THEN (sls_quantity * ABS(sls_price))
	ELSE sls_sales
END AS NEW_sls_sales,
sls_price,
CASE WHEN sls_price <=0 OR sls_price IS NULL THEN sls_sales/NULLIF(sls_quantity,0)
	ELSE sls_price
END AS NEW_sls_price
from silver.crm_sales_details
where sls_sales != (sls_quantity * sls_price)
or sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <=0 OR sls_quantity <=0 OR sls_price <=0;


===========================================================================
Quality check: silver.erp_cust
============================================================================

--Quality check for erp_cust table:

--to match the cid with erp table and crm cust info table for establishing relationship.
select case when cid like 'NAS%' THEN substring(cid,4,LEN(cid)) 
	ELSE cid
end as cid from silver.erp_cust_az12 
where (case when cid like 'NAS%' THEN substring(cid,4,LEN(cid)) 
	ELSE cid
end) not in (select distinct cst_key from silver.crm_cust_info);

-- To replace NULLS if the birthdate is greater than today:
select cid,bdate,CASE WHEN bdate > GETDATE() THEN NULL
	ELSE bdate
END AS bdate from silver.erp_cust_az12  where bdate > GETDATE();

-- Data standardization and integrity:
SELECT DISTINCT
CASE WHEN UPPER(gen) IN ('F','FEMALE') THEN 'Female'
	WHEN UPPER(gen) IN ('M','MALE') THEN 'Male'
	WHEN gen IS NULL or gen = '' THEN 'n/a'
END AS gen
from silver.erp_cust_az12;


===========================================================================
Quality check: silver.erp_loc
============================================================================

--Quality check for erp_loc table:

--To find the cid is present in crm cust table or not. Also we have to match the coln according to crm table :
select REPLACE(cid,'-','') AS cid from silver.erp_loc_a101
where REPLACE(cid,'-','') not in (select cst_key from silver.crm_cust_info)

--Data Standardization and integrity:
select distinct cntry as old_cntry,
case  when  cntry in ('US','USA','United States') THEN 'United States'
	WHEN cntry in ('DE','Germany') THEN 'Germany'
	WHEN cntry IS NULL or cntry = '' then 'n/a'
	ELSE cntry 
END as cntry
from silver.erp_loc_a101;


===========================================================================
Quality check: silver.erp_px_cat_g1v2
============================================================================

--To check if the id is not with spaces and is it matching with the id in crm prd info table:
select id from silver.erp_px_cat_g1v2 where id != trim(id) AND id not in (
select cat_id from silver.crm_prd_info);


select distinct cat from silver.erp_px_cat_g1v2;
select distinct subcat from silver.erp_px_cat_g1v2;
select distinct maintenance from silver.erp_px_cat_g1v2;
