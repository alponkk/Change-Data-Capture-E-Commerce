{{ config(materialized='table') }}

WITH orders_with_line_items AS (
    SELECT
        order_id,
        customer_id,
        ordered_at,
        order_status,
        line_items,
        toDate(ordered_at) AS order_date
    FROM {{ ref('stg_orders') }}
    WHERE 
        order_status IN ('shipped', 'delivered', 'pending')  -- Include valid orders
        AND line_items IS NOT NULL 
        AND line_items != ''
        AND line_items != '[]'  -- Exclude empty arrays
),

-- Unnest line items and calculate individual line totals
line_items_unnested AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        -- Extract individual line items from the JSON array
        arrayJoin(JSONExtract(line_items, 'Array(String)')) AS line_item_json
    FROM orders_with_line_items
    WHERE JSONLength(line_items) > 0  -- Ensure array is not empty
),

-- Parse each line item and calculate line totals
line_items_parsed AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        -- Extract quantity and price from each line item
        toFloat64OrZero(JSONExtractString(line_item_json, 'quantity')) AS quantity,
        toFloat64OrZero(JSONExtractString(line_item_json, 'price')) AS price,
        -- Calculate line total (quantity * price)
        toFloat64OrZero(JSONExtractString(line_item_json, 'quantity')) * 
        toFloat64OrZero(JSONExtractString(line_item_json, 'price')) AS line_total
    FROM line_items_unnested
    WHERE 
        -- Filter out invalid line items
        JSONExtractString(line_item_json, 'quantity') != ''
        AND JSONExtractString(line_item_json, 'price') != ''
        AND toFloat64OrZero(JSONExtractString(line_item_json, 'quantity')) > 0
        AND toFloat64OrZero(JSONExtractString(line_item_json, 'price')) > 0
),

-- Calculate total order values
order_totals AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        sum(line_total) AS order_total_value,
        count() AS line_items_count
    FROM line_items_parsed
    GROUP BY order_id, customer_id, order_date
    HAVING order_total_value > 0  -- Exclude orders with zero value
),

-- Final daily aggregation
daily_sales AS (
    SELECT
        order_date,
        
        -- Total revenue for the day
        sum(order_total_value) AS total_revenue,
        
        -- Total number of orders placed
        count(DISTINCT order_id) AS total_orders_placed,
        
        -- Total unique customers who placed orders
        uniq(customer_id) AS total_unique_customers,
        
        -- Additional helpful metrics
        avg(order_total_value) AS avg_order_value,
        sum(line_items_count) AS total_line_items,
        
        -- Metadata
        now() AS _calculated_at
        
    FROM order_totals
    GROUP BY order_date
)

SELECT 
    order_date,
    total_revenue,
    total_orders_placed,
    total_unique_customers,
    avg_order_value,
    total_line_items,
    _calculated_at
FROM daily_sales
ORDER BY order_date DESC 