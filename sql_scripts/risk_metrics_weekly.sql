DROP TABLE IF EXISTS risk_metrics_weekly;

WITH weekly_price_stats AS (
    SELECT
        date_trunc('week', date_time)::date AS week_start,
        AVG(price_actual) AS avg_price,
        MIN(price_actual) AS low,
        MAX(price_actual) AS high
    FROM energy
    GROUP BY week_start
),
weekly_opening_prices AS (
    SELECT DISTINCT ON (week_start)
        week_start,
        price_actual AS op
    FROM (
        SELECT
            date_trunc('week', date_time)::date AS week_start,
            date_time,
            price_actual
        FROM energy
        ORDER BY week_start, date_time -- Get the first record of the week
    ) subquery
),
weekly_closing_prices AS (
    SELECT DISTINCT ON (week_start)
        week_start,
        price_actual AS cp
    FROM (
        SELECT
            date_trunc('week', date_time)::date AS week_start,
            date_time,
            price_actual
        FROM energy
        ORDER BY week_start, date_time DESC -- Get the last record of the week
    ) subquery
),
volatility AS (
    SELECT
        week_start,
        (
            SELECT STDDEV(e.price_actual)
            FROM energy AS e
            WHERE e.date_time BETWEEN wps.week_start - INTERVAL '4 weeks' AND wps.week_start
        ) AS volatility_4w
    FROM weekly_price_stats AS wps
),
var AS (
    SELECT
        week_start,
        (
            SELECT 
                PERCENTILE_CONT(0.05) 
                WITHIN GROUP (ORDER BY e.price_actual)
            FROM energy AS e
            WHERE e.date_time BETWEEN wps.week_start - INTERVAL '4 weeks' AND wps.week_start
        ) AS var_95
    FROM weekly_price_stats AS wps
),
cvar AS (
    SELECT
        week_start,
        (
            SELECT 
                AVG(e.price_actual)
            FROM energy AS e
            WHERE 
                e.date_time BETWEEN v.week_start - INTERVAL '4 weeks' AND v.week_start
                AND e.price_actual <= v.var_95
        ) AS cvar_95
    FROM var AS v
)

SELECT
    wps.week_start,
    wps.avg_price,
    wps.low,
    wps.high,
    wo.op,
    wc.cp,
    v.volatility_4w,
    var.var_95,
    cvar.cvar_95
INTO 
    risk_metrics_weekly
FROM
    weekly_price_stats AS wps
JOIN
    volatility AS v ON wps.week_start = v.week_start
JOIN
    var ON wps.week_start = var.week_start
JOIN
    cvar ON wps.week_start = cvar.week_start
JOIN
    weekly_opening_prices AS wo ON wps.week_start = wo.week_start
JOIN
    weekly_closing_prices AS wc ON wps.week_start = wc.week_start;

SELECT * FROM risk_metrics_weekly;