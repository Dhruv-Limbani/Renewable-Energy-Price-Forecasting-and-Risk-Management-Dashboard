DROP TABLE IF EXISTS risk_metrics_hourly;
with volatility AS(
	SELECT
		date_time,
		(
			SELECT 
				STDDEV(e1.price_actual)
			FROM 
				energy AS e1
			WHERE e1.date_time BETWEEN e2.date_time - INTERVAL '23 hours' AND e2.date_time
		) AS volatility_1d
	FROM energy AS e2
),
var AS (
	select
		date_time,
        (
			SELECT 
				PERCENTILE_CONT(0.05) 
			WITHIN GROUP 
				(ORDER BY e1.price_actual)
         	FROM 
			 	energy AS e1
         	WHERE e1.date_time BETWEEN e2.date_time - INTERVAL '23 hours' AND e2.date_time
        ) 	AS var_95
	FROM energy AS e2
),
cvar AS(
	SELECT
		date_time,
		(
			SELECT 
				AVG(e.price_actual)
			FROM 
				energy AS e
			WHERE 
				(e.date_time BETWEEN v.date_time - INTERVAL '23 hours' AND v.date_time)
			AND
				(e.price_actual<=v.var_95)
		) AS cvar_95
	FROM var AS v
)
SELECT
	e.date_time,
	e.price_actual,
	v.volatility_1d,
	var.var_95,
	cvar.cvar_95
INTO 
	risk_metrics_hourly
FROM
	energy AS e
JOIN
	volatility AS v
ON e.date_time = v.date_time
JOIN
	var
ON e.date_time = var.date_time
JOIN
	cvar
ON e.date_time = cvar.date_time;
SELECT * FROM risk_metrics_hourly;