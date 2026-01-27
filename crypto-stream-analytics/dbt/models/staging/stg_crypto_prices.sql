-- Staging model for raw crypto prices from Bronze layer
-- Source: Delta Lake bronze/crypto_prices

{{ config(materialized='view') }}

WITH source AS (
    SELECT *
    FROM delta_scan('{{ var("delta_lake_path", "../data/delta") }}/bronze/crypto_prices')
),

cleaned AS (
    SELECT
        symbol,
        price,
        volume,
        price_change_percent,
        high_24h,
        low_24h,
        event_time,
        ingested_at,
        kafka_partition,
        kafka_offset,

        -- Data quality flags
        CASE
            WHEN price <= 0 THEN TRUE
            WHEN volume < 0 THEN TRUE
            WHEN ABS(price_change_percent) > {{ var('max_price_change_pct') }} THEN TRUE
            ELSE FALSE
        END AS is_anomaly,

        -- Derived fields
        DATE_TRUNC('hour', event_time) AS event_hour,
        DATE_TRUNC('day', event_time) AS event_date

    FROM source
    WHERE
        symbol IS NOT NULL
        AND price IS NOT NULL
        AND event_time IS NOT NULL
)

SELECT * FROM cleaned
