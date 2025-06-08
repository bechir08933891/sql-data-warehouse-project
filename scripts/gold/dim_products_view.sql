-- ==================================================
-- dimension products : discriptive
-- ==================================================
if OBJECT_ID('gold.dim_products', 'V') is not null
	drop view gold.dim_products;
go
create view gold.dim_products as
select
	-- surrogate key 
	ROW_NUMBER() over (order by pn.prd_start_dt, pn.prd_key) as product_key,
	pn.prd_id as product_id,
	pn.prd_key as product_number,
	pn.prd_nm as product_name,
	pn.cat_id category_id,
	pc.cat as category,
	pc.subcat as subcategory,
	pc.maintenance,
	pn.prd_cost as cost,
	pn.prd_line as product_line,
	pn.prd_start_dt as start_dates
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc
on pn.cat_id = pc.id
where prd_end_dt is null -- filter out all historical data
