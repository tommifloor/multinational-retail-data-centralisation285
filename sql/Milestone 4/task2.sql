-- Find store count by locality
SELECT 
	locality, 
	COUNT(store_code) AS store_no_count
FROM
	dim_store_details
GROUP BY
	locality
ORDER BY
	store_no_count DESC, locality ASC
LIMIT
	7
;