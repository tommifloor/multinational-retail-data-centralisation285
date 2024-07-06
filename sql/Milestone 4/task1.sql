-- Select total stores in each country
SELECT
	country_code AS country, 
	COUNT(store_code) AS total_no_stores
FROM 
	dim_store_details
GROUP BY
	country_code