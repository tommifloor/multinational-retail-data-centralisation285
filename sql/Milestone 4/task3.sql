-- Find months with largest sales. Sort by sales
SELECT ROUND(SUM(product_price*product_quantity)::NUMERIC, 2::SMALLINT) AS total_sales,
		"month"
FROM
	orders_table AS ot
INNER JOIN
	dim_date_times AS dt ON dt.date_uuid = ot.date_uuid
INNER JOIN
	dim_products AS dp ON dp.product_code = ot.product_code
GROUP BY
	"month"
ORDER BY
	total_sales DESC
LIMIT
	6
;