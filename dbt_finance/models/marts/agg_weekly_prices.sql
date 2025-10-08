-- models/marts/agg_weekly_prices.sql

SELECT
  symbol,
  DATE_TRUNC('week', trading_date) AS week_start,
  AVG(close) AS avg_close,
  AVG(percent_change) AS avg_percent_change
FROM {{ ref('fact_stock_prices') }}
GROUP BY symbol, week_start
