{{ config(materialized='table') }}

WITH latest_products AS (
    SELECT 
        product_id,
        product_name,
        price,
        stock_quantity,
        _operation,
        _event_timestamp_ms,
        _extracted_at,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY _event_timestamp_ms DESC) AS rn
    FROM {{ ref('stg_products') }}
    WHERE _operation IN ('r', 'c', 'u')  -- read, create, update operations only
),

product_dimension AS (
    SELECT
        product_id,
        product_name,
        price,
        stock_quantity,
        CASE 
            WHEN price < 50 THEN 'Low'
            WHEN price < 200 THEN 'Medium'
            ELSE 'High'
        END AS price_category,
        CASE 
            WHEN stock_quantity = 0 THEN 'Out of Stock'
            WHEN stock_quantity < 10 THEN 'Low Stock'
            WHEN stock_quantity < 50 THEN 'Medium Stock'
            ELSE 'High Stock'
        END AS inventory_status,
        CASE 
            WHEN stock_quantity = 0 THEN TRUE
            ELSE FALSE
        END AS is_out_of_stock,
        round(price, 2) AS price_rounded,
        _event_timestamp_ms,
        _extracted_at,
        now() AS _processed_at
    FROM latest_products
    WHERE rn = 1  -- Get only the latest version of each product
)

SELECT 
    product_id,
    product_name,
    price,
    price_rounded,
    price_category,
    stock_quantity,
    inventory_status,
    is_out_of_stock,
    _event_timestamp_ms,
    _extracted_at,
    _processed_at
FROM product_dimension
ORDER BY product_name 