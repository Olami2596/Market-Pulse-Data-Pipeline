-- models/marts/dim_stock.sql

WITH distinct_symbols AS (
    SELECT DISTINCT symbol
    FROM {{ ref('stg_alphavantage') }}
)

SELECT
    symbol,
    CASE
        WHEN symbol = 'AAPL' THEN 'Apple Inc.'
        WHEN symbol = 'MSFT' THEN 'Microsoft Corporation'
        WHEN symbol = 'GOOGL' THEN 'Alphabet Inc.'
        WHEN symbol = 'AMZN' THEN 'Amazon.com, Inc.'
        WHEN symbol = 'META' THEN 'Meta Platforms, Inc.'
        WHEN symbol = 'NVDA' THEN 'NVIDIA Corporation'
        WHEN symbol = 'TSLA' THEN 'Tesla, Inc.'
        WHEN symbol = 'NFLX' THEN 'Netflix, Inc.'
        WHEN symbol = 'BRK.B' THEN 'Berkshire Hathaway Inc.'
        WHEN symbol = 'JPM' THEN 'JPMorgan Chase & Co.'
        ELSE 'Unknown Company'
    END AS company_name,

    CASE
        WHEN symbol IN ('AAPL', 'MSFT', 'GOOGL', 'META', 'NVDA', 'TSLA', 'NFLX') THEN 'Technology'
        WHEN symbol IN ('BRK.B', 'JPM') THEN 'Financials'
        WHEN symbol = 'AMZN' THEN 'Consumer Discretionary'
        ELSE 'Other'
    END AS sector
FROM distinct_symbols
