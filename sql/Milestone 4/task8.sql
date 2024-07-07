-- Sales in Germany  by store type
SELECT
	ROUND(
		SUM(ot.product_quantity * dp.product_price
			)::NUMERIC, 2::SMALLINT
		) AS total_sales,
	store_type,
	country_code
FROM
	dim_store_details AS ds
INNER JOIN
	orders_table AS ot ON ds.store_code = ot.store_code
INNER JOIN
	dim_products AS dp ON ot.product_code = dp.product_code
WHERE
	country_code = 'DE'
GROUP BY
	store_type, country_code
ORDER BY
	total_sales ASC