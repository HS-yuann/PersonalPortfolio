-- Staging model for OHLC candles from Silver layer
-- Source: Delta Lake silver/ohlc_1min

{{ config(materialized='view') }}

WITH source AS (
    SELECT *
    FROM delta_scan('{{ var("delta_lake_path", "../data/delta") }}/silver/ohlc_1min')
),

validated AS (
    SELECT
        symbol,
        window_start,
        window_end,
        open,
        high,
        low,
        close,
        volume,
        trade_count,
        vwap,
        price_volatility,
        candle_change_pct,
        processed_at,

        -- OHLC integrity check
        CASE
            WHEN high < low THEN TRUE
            WHEN open > high OR open < low THEN TRUE
            WHEN close > high OR close < low THEN TRUE
            ELSE FALSE
        END AS has_integrity_issue,

        -- Candle type classification
        CASE
            WHEN close > open THEN 'BULLISH'
            WHEN close < open THEN 'BEARISH'
            ELSE 'DOJI'
        END AS candle_type,

        -- Body and wick calculations
        ABS(close - open) AS body_size,
        high - GREATEST(open, close) AS upper_wick,
        LEAST(open, close) - low AS lower_wick

    FROM source
    WHERE
        symbol IS NOT NULL
        AND window_start IS NOT NULL
)

SELECT * FROM validated
