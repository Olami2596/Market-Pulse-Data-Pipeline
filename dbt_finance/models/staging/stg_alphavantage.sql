-- models/staging/stg_alphavantage.sql

WITH source AS (
    SELECT 
        SYMBOL,
        DATA:"Time Series (Daily)" AS time_series
    FROM {{ source('raw', 'RAW_ALPHAVANTAGE') }}
),

flattened AS (
    SELECT
        SYMBOL,
        key::date AS trading_date,
        value:"1. open"::float AS open,
        value:"2. high"::float AS high,
        value:"3. low"::float AS low,
        value:"4. close"::float AS close,
        value:"5. volume"::int AS volume
    FROM source,
    LATERAL FLATTEN(input => time_series)
)

SELECT * FROM flattened
