{{ config(materialized='table') }}

SELECT
    order_id,
    customer_id,

    -- Normalize status to lowercase so 'Delivered' and 'delivered' don't become
    -- two separate values downstream
    LOWER(TRIM(order_status))                           AS order_status,

    -- Cast raw strings to timestamps. Bronze stored everything as text.
    TRY_CAST(order_purchase_timestamp AS TIMESTAMP)    AS order_purchase_timestamp,
    TRY_CAST(order_approved_at AS TIMESTAMP)           AS order_approved_at,
    TRY_CAST(order_delivered_carrier_date AS TIMESTAMP)AS order_delivered_carrier_date,
    TRY_CAST(order_delivered_customer_date AS TIMESTAMP)AS order_delivered_customer_date,
    TRY_CAST(order_estimated_delivery_date AS TIMESTAMP)AS order_estimated_delivery_date,

    -- Carry the ingestion timestamp forward for lineage tracking
    _ingested_at

FROM main.bronze_orders

-- Drop rows where the two most critical fields are missing.
-- An order without an ID or customer is useless to every downstream model.
WHERE order_id IS NOT NULL
  AND customer_id IS NOT NULL