DROP TABLE IF EXISTS risk_metrics_weekly;

WITH weekly_prices AS (
    SELECT 
        date_trunc('week', date_time)::date AS week,  
        AVG(price_actual) AS avg_price  
    FROM energy
    GROUP BY date_trunc('week', date_time)
),
risk_metrics AS (
    SELECT 
        week,
        avg_price,
        STDDEV(avg_price) OVER (
            ORDER BY week
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) AS volatility_4w
    FROM weekly_prices
),
var_metrics AS (
    SELECT 
        week,
        PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY avg_price) AS var_95
    FROM weekly_prices
    GROUP BY week
)
SELECT 
    r.week,
    r.avg_price,  
    r.volatility_4w,
    v.var_95,

    (SELECT AVG(sub.avg_price) 
     FROM weekly_prices AS sub 
     WHERE sub.week BETWEEN r.week - INTERVAL '4 weeks' AND r.week
     AND sub.avg_price <= v.var_95
    ) AS cvar_95
INTO risk_metrics_weekly
FROM risk_metrics AS r
JOIN var_metrics AS v ON r.week = v.week;

SELECT * FROM risk_metrics_weekly;