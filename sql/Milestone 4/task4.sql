-- Online vs offline sales
SELECT count(date_uuid) AS number_of_sales, 
	   SUM(product_quantity) AS product_quantity_count, 
	   (CASE WHEN store_code = 'WEB-1388012W' THEN 'Web' 
			 ELSE 'Offline' 
		END) AS "location"
FROM 
	orders_table AS ot
GROUP BY
	"location"
ORDER BY
	number_of_sales
;