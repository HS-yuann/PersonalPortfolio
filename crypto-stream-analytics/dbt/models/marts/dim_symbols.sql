-- Dimension table for trading symbols
-- Contains metadata about each cryptocurrency

{{ config(materialized='table') }}

WITH symbols AS (
    SELECT DISTINCT symbol
    FROM {{ ref('stg_crypto_prices') }}
),

enriched AS (
    SELECT
        symbol,

        -- Extract base and quote currencies
        CASE
            WHEN symbol LIKE '%USDT' THEN REPLACE(symbol, 'USDT', '')
            WHEN symbol LIKE '%USD' THEN REPLACE(symbol, 'USD', '')
            WHEN symbol LIKE '%BTC' THEN REPLACE(symbol, 'BTC', '')
            ELSE symbol
        END AS base_currency,

        CASE
            WHEN symbol LIKE '%USDT' THEN 'USDT'
            WHEN symbol LIKE '%USD' THEN 'USD'
            WHEN symbol LIKE '%BTC' THEN 'BTC'
            ELSE 'UNKNOWN'
        END AS quote_currency,

        -- Cryptocurrency names (hardcoded for known symbols)
        CASE
            WHEN symbol = 'BTCUSDT' THEN 'Bitcoin'
            WHEN symbol = 'ETHUSDT' THEN 'Ethereum'
            WHEN symbol = 'BNBUSDT' THEN 'Binance Coin'
            WHEN symbol = 'SOLUSDT' THEN 'Solana'
            WHEN symbol = 'XRPUSDT' THEN 'Ripple'
            ELSE 'Unknown'
        END AS currency_name,

        -- Market tier classification
        CASE
            WHEN symbol IN ('BTCUSDT', 'ETHUSDT') THEN 'TIER_1'
            WHEN symbol IN ('BNBUSDT', 'SOLUSDT', 'XRPUSDT') THEN 'TIER_2'
            ELSE 'TIER_3'
        END AS market_tier,

        TRUE AS is_active,
        CURRENT_TIMESTAMP AS updated_at

    FROM symbols
)

SELECT * FROM enriched
