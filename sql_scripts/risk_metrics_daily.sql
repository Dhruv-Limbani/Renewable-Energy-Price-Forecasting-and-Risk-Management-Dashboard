DROP TABLE IF EXISTS risk_metrics_daily;

WITH daily_prices AS (
    SELECT 
        date_time::date AS day,  
        AVG(price_actual) AS avg_price
    FROM energy
    GROUP BY date_time::date
),
risk_metrics AS (
    SELECT 
        day,
        avg_price,

        STDDEV(avg_price) OVER (
            ORDER BY day
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS volatility_7d
    FROM daily_prices
),
var_metrics AS (
    SELECT 
        day,
        PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY avg_price) AS var_95
    FROM daily_prices
    GROUP BY day
)
SELECT 
    r.day,
    r.avg_price,  
    r.volatility_7d,
    v.var_95,

    (SELECT AVG(sub.avg_price) 
     FROM daily_prices AS sub 
     WHERE sub.day BETWEEN r.day - INTERVAL '7 days' AND r.day
     AND sub.avg_price <= v.var_95
    ) AS cvar_95
INTO risk_metrics_daily
FROM risk_metrics AS r
JOIN var_metrics AS v ON r.day = v.day;

SELECT * FROM risk_metrics_daily;