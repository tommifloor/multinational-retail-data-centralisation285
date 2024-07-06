-- Sales by store_type
WITH sales_by_type AS (
	SELECT
	ds.store_type,
	SUM(ot.product_quantity * dp.product_price) AS total_sales,
	COUNT(*) * 100.0 / sum(count(*)) over() AS "percentage(%)"
	FROM
	orders_table AS ot
	INNER JOIN
	dim_products AS dp ON dp.product_code = ot.product_code
	INNER JOIN
	dim_store_details AS ds ON ds.store_code = ot.store_code
	GROUP BY
	ds.store_type
	)
SELECT 
	store_type,
	ROUND(total_sales::NUMERIC, 2::SMALLINT),
	ROUND(SUM("percentage(%)")::NUMERIC, 2::SMALLINT)
FROM 
	sales_by_type
GROUP BY
	store_type, total_sales
ORDER BY
	total_sales DESC
;