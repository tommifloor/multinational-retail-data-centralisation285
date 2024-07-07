-- Sales in Germany  by store type
WITH full_date AS (
	SELECT
		"year",
		"month",
		"day",
		MAKE_DATE(
			"year"::SMALLINT,
			"month"::SMALLINT,
			"day"::SMALLINT
			) + "timestamp"::INTERVAL AS date_time
	FROM
		dim_date_times
	ORDER BY
		date_time
), time_difference_table AS (
	SELECT
		"year",
		"month",
		"day",
		date_time,
		LEAD(date_time) OVER (
			PARTITION BY "year"
			ORDER BY date_time
			) - date_time AS time_difference
	FROM
		full_date
	ORDER BY
		date_time
)

SELECT
	"year",
	AVG(time_difference) AS actual_time_taken
FROM
	time_difference_table
GROUP BY
	"year"
ORDER BY
	actual_time_taken DESC
LIMIT 5
;