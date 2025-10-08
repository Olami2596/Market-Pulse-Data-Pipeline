-- models/marts/fact_stock_prices.sql

WITH base AS (
    -- Pull in flattened daily prices from staging
    SELECT * FROM {{ ref('stg_alphavantage') }}
),

joined AS (
    -- Join with dimension table to enrich with company and sector info
    SELECT
        b.symbol,
        d.company_name,
        d.sector,
        b.trading_date,
        b.open,
        b.high,
        b.low,
        b.close,
        b.volume,
        ROUND(b.close - b.open, 2) AS daily_change,
        ROUND(((b.close - b.open) / NULLIF(b.open, 0)) * 100, 2) AS percent_change
    FROM base b
    LEFT JOIN {{ ref('dim_stock') }} d
      ON b.symbol = d.symbol
)

-- Output enriched dataset
SELECT * FROM joined
