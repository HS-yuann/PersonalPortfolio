-- Hourly aggregation statistics for BI dashboards
-- Optimized for time-series analysis

{{ config(
    materialized='table',
    partition_by='event_date'
) }}

WITH candles AS (
    SELECT * FROM {{ ref('stg_ohlc_candles') }}
    WHERE NOT has_integrity_issue
),

hourly_stats AS (
    SELECT
        symbol,
        DATE_TRUNC('hour', window_start) AS hour_start,
        DATE_TRUNC('day', window_start) AS event_date,

        -- OHLC for the hour
        FIRST_VALUE(open) OVER (
            PARTITION BY symbol, DATE_TRUNC('hour', window_start)
            ORDER BY window_start
        ) AS hour_open,

        MAX(high) AS hour_high,
        MIN(low) AS hour_low,

        LAST_VALUE(close) OVER (
            PARTITION BY symbol, DATE_TRUNC('hour', window_start)
            ORDER BY window_start
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS hour_close,

        -- Aggregations
        SUM(volume) AS total_volume,
        SUM(trade_count) AS total_trades,
        AVG(vwap) AS avg_vwap,
        AVG(price_volatility) AS avg_volatility,

        -- Candle analysis
        COUNT(*) AS candle_count,
        SUM(CASE WHEN candle_type = 'BULLISH' THEN 1 ELSE 0 END) AS bullish_candles,
        SUM(CASE WHEN candle_type = 'BEARISH' THEN 1 ELSE 0 END) AS bearish_candles,
        SUM(CASE WHEN candle_type = 'DOJI' THEN 1 ELSE 0 END) AS doji_candles

    FROM candles
    GROUP BY
        symbol,
        DATE_TRUNC('hour', window_start),
        DATE_TRUNC('day', window_start),
        open, close  -- For window functions
),

final AS (
    SELECT DISTINCT
        symbol,
        hour_start,
        event_date,
        hour_open,
        hour_high,
        hour_low,
        hour_close,

        -- Price change
        (hour_close - hour_open) / NULLIF(hour_open, 0) * 100 AS hour_change_pct,

        -- Volume & trades
        total_volume,
        total_trades,
        avg_vwap,
        avg_volatility,

        -- Candle distribution
        candle_count,
        bullish_candles,
        bearish_candles,
        doji_candles,
        bullish_candles * 1.0 / NULLIF(candle_count, 0) AS bullish_ratio,

        -- Market sentiment
        CASE
            WHEN bullish_candles > bearish_candles * 1.5 THEN 'STRONG_BULLISH'
            WHEN bullish_candles > bearish_candles THEN 'BULLISH'
            WHEN bearish_candles > bullish_candles * 1.5 THEN 'STRONG_BEARISH'
            WHEN bearish_candles > bullish_candles THEN 'BEARISH'
            ELSE 'NEUTRAL'
        END AS hour_sentiment,

        CURRENT_TIMESTAMP AS computed_at

    FROM hourly_stats
)

SELECT * FROM final
