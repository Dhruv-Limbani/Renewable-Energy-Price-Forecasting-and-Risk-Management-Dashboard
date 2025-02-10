DROP TABLE IF EXISTS risk_metrics_daily;
WITH daily_price_stats AS(
	SELECT
		date_trunc('day',date_time)::date AS day,
		AVG(price_actual) AS avg_price,
		MIN(price_actual) AS low,
		MAX(price_actual) AS high
	FROM
		energy
	GROUP BY 
		day
),
daily_opening_prices AS(
	SELECT
		date_trunc('day',date_time)::date AS day,
		price_actual AS op
	FROM
		energy
	WHERE
		EXTRACT(HOUR FROM date_time) = 0
),
daily_closing_prices AS(
	SELECT
		date_trunc('day',date_time)::date AS day,
		price_actual AS cp
	FROM
		energy
	WHERE
		EXTRACT(HOUR FROM date_time) = 23
),
volatility AS(
	SELECT
		day,
		(
			SELECT 
				STDDEV(e.price_actual)
			FROM 
				energy AS e
			WHERE e.date_time BETWEEN dps.day - INTERVAL '6 days' AND dps.day
		) AS volatility_7d
	FROM daily_price_stats AS dps
),
var AS (
	select
		day,
        (
			SELECT 
				PERCENTILE_CONT(0.05) 
			WITHIN GROUP 
				(ORDER BY e.price_actual)
         	FROM 
			 	energy AS e
         	WHERE e.date_time BETWEEN dps.day - INTERVAL '6 days' AND dps.day
        ) 	AS var_95
	FROM daily_price_stats AS dps
),
cvar AS(
	SELECT
		day,
		(
			SELECT 
				AVG(e.price_actual)
			FROM 
				energy AS e
			WHERE 
				(e.date_time BETWEEN v.day - INTERVAL '6 days' AND v.day)
			AND
				(e.price_actual<=v.var_95)
		) AS cvar_95
	FROM var AS v
)
SELECT
	dps.day,
	dps.avg_price,
	dps.low,
	dps.high,
	o.op,
	c.cp,
	v.volatility_7d,
	var.var_95,
	cvar.cvar_95
INTO 
	risk_metrics_daily
FROM
	daily_price_stats AS dps
JOIN
	volatility AS V
ON dps.day = v.day
JOIN
	var
ON dps.day = var.day
JOIN
	cvar
ON dps.day = cvar.day
JOIN
	daily_opening_prices AS o
ON dps.day = o.day
JOIN
	daily_closing_prices AS c
ON dps.day = c.day;

SELECT * FROM risk_metrics_daily;