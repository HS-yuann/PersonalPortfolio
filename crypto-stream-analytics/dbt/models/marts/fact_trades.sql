-- Fact table for trades/price events
-- Optimized for analytical queries

{{ config(
    materialized='table',
    partition_by='event_date'
) }}

WITH prices AS (
    SELECT * FROM {{ ref('stg_crypto_prices') }}
    WHERE NOT is_anomaly
),

with_lag AS (
    SELECT
        *,
        LAG(price) OVER (PARTITION BY symbol ORDER BY event_time) AS prev_price,
        LAG(volume) OVER (PARTITION BY symbol ORDER BY event_time) AS prev_volume
    FROM prices
),

enriched AS (
    SELECT
        -- Keys
        {{ dbt_utils.generate_surrogate_key(['symbol', 'event_time']) }} AS trade_id,
        symbol,
        event_time,
        event_hour,
        event_date,

        -- Price metrics
        price,
        prev_price,
        price - COALESCE(prev_price, price) AS price_change,
        CASE
            WHEN prev_price > 0 THEN (price - prev_price) / prev_price * 100
            ELSE 0
        END AS price_change_pct,

        -- Volume metrics
        volume,
        prev_volume,
        volume - COALESCE(prev_volume, volume) AS volume_change,

        -- 24h context
        high_24h,
        low_24h,
        price_change_percent AS change_24h_pct,

        -- Position within 24h range
        CASE
            WHEN high_24h > low_24h THEN (price - low_24h) / (high_24h - low_24h) * 100
            ELSE 50
        END AS position_in_range_pct,

        -- Metadata
        ingested_at,
        kafka_partition,
        kafka_offset

    FROM with_lag
)

SELECT * FROM enriched
