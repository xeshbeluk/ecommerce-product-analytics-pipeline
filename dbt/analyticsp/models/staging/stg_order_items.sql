{{ config(materialized='table') }}

SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    TRY_CAST(shipping_limit_date AS TIMESTAMP) AS shipping_limit_date,
    TRY_CAST(price AS DOUBLE) AS price,
    TRY_CAST(freight_value AS DOUBLE) AS freight_value,
-- Carry the ingestion timestamp forward for lineage tracking
    _ingested_at
FROM main.bronze_order_items

WHERE order_id IS NOT NULL
  AND product_id IS NOT NULL