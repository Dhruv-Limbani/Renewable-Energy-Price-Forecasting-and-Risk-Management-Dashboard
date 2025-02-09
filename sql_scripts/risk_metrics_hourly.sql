drop table if exists risk_metrics_hourly;
WITH risk_metrics AS (
    SELECT 
        date_time,
        price_actual,

        -- Compute 7-day rolling volatility (168 hours)
        STDDEV(price_actual) OVER (
            ORDER BY date_time 
            ROWS BETWEEN 167 PRECEDING AND CURRENT ROW
        ) AS volatility_7d
    FROM energy
),
var_metrics AS (
    SELECT 
        date_trunc('hour', date_time) AS hour,
        PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY price_actual) AS var_95
    FROM energy
    GROUP BY date_trunc('hour', date_time)
)
SELECT 
    r.date_time,
    r.price_actual,
    r.volatility_7d,
    v.var_95,
    
    -- Compute CVaR (Conditional Value at Risk 95%)
    (SELECT AVG(sub.price_actual) 
     FROM risk_metrics AS sub 
     WHERE sub.date_time BETWEEN r.date_time - INTERVAL '7 days' AND r.date_time
     AND sub.price_actual <= v.var_95
    ) AS cvar_95
into risk_metrics_hourly
FROM risk_metrics AS r
JOIN var_metrics AS v ON date_trunc('hour', r.date_time) = v.hour;

