{{ config(materialized='table') }}

WITH orders_with_line_items AS (
    SELECT 
        order_id,
        customer_id,
        ordered_at,
        order_status,
        line_items,
        toDate(ordered_at) AS order_date,
        _operation,
        _event_timestamp_ms,
        _extracted_at
    FROM {{ ref('stg_orders') }}
    WHERE order_status IN ('shipped', 'delivered', 'pending') 
      AND line_items IS NOT NULL 
      AND line_items != '' 
      AND line_items != '[]'
),

line_items_unnested AS (
    SELECT 
        order_id,
        customer_id,
        ordered_at,
        order_date,
        order_status,
        _operation,
        _event_timestamp_ms,
        _extracted_at,
        arrayJoin(JSONExtract(line_items, 'Array(String)')) AS line_item_json
    FROM orders_with_line_items 
    WHERE JSONLength(line_items) > 0
),

line_items_parsed AS (
    SELECT 
        order_id,
        customer_id,
        ordered_at,
        order_date,
        order_status,
        _operation,
        _event_timestamp_ms,
        _extracted_at,
        JSONExtractString(line_item_json, 'product_id.$oid') AS product_id,
        toFloat64OrZero(JSONExtractString(line_item_json, 'quantity')) AS quantity,
        toFloat64OrZero(JSONExtractString(line_item_json, 'price')) AS unit_price,
        toFloat64OrZero(JSONExtractString(line_item_json, 'quantity')) * toFloat64OrZero(JSONExtractString(line_item_json, 'price')) AS line_total
    FROM line_items_unnested
    WHERE JSONExtractString(line_item_json, 'quantity') != '' 
      AND JSONExtractString(line_item_json, 'price') != ''
      AND toFloat64OrZero(JSONExtractString(line_item_json, 'quantity')) > 0
      AND toFloat64OrZero(JSONExtractString(line_item_json, 'price')) > 0
),

orders_enriched AS (
    SELECT 
        o.order_id,
        o.customer_id,
        o.product_id,
        o.ordered_at,
        o.order_date,
        o.order_status,
        o.quantity,
        o.unit_price,
        o.line_total,
        o._operation,
        o._event_timestamp_ms,
        o._extracted_at,
        
        -- Customer information
        c.full_name AS customer_name,
        c.first_name AS customer_first_name,
        c.last_name AS customer_last_name,
        c.email AS customer_email,
        c.customer_segment,
        c.days_since_registration,
        
        -- Product information
        p.product_name,
        p.price AS current_product_price,
        p.price_category,
        p.inventory_status,
        p.is_out_of_stock,
        
        -- Calculated fields
        abs(o.unit_price - p.price) AS price_difference,
        CASE 
            WHEN abs(o.unit_price - p.price) > 0.01 THEN TRUE
            ELSE FALSE
        END AS has_price_variance,
        
        now() AS _processed_at
        
    FROM line_items_parsed o
    LEFT JOIN {{ ref('dim_customers') }} c ON o.customer_id = c.customer_id
    LEFT JOIN {{ ref('dim_products') }} p ON o.product_id = p.product_id
),

order_metrics AS (
    SELECT 
        *,
        -- Order-level aggregations
        sum(line_total) OVER (PARTITION BY order_id) AS order_total_value,
        count(*) OVER (PARTITION BY order_id) AS items_in_order,
        avg(unit_price) OVER (PARTITION BY order_id) AS avg_item_price_in_order,
        
        -- Customer-level aggregations
        count(DISTINCT order_id) OVER (PARTITION BY customer_id) AS customer_lifetime_orders,
        sum(line_total) OVER (PARTITION BY customer_id) AS customer_lifetime_value,
        
        -- Product-level aggregations
        sum(quantity) OVER (PARTITION BY product_id) AS product_total_quantity_sold,
        sum(line_total) OVER (PARTITION BY product_id) AS product_total_revenue
        
    FROM orders_enriched
)

SELECT 
    order_id,
    customer_id,
    product_id,
    ordered_at,
    order_date,
    order_status,
    quantity,
    unit_price,
    line_total,
    
    -- Customer details
    customer_name,
    customer_first_name,
    customer_last_name,
    customer_email,
    customer_segment,
    days_since_registration,
    
    -- Product details
    product_name,
    current_product_price,
    price_category,
    inventory_status,
    is_out_of_stock,
    
    -- Price analysis
    price_difference,
    has_price_variance,
    
    -- Aggregated metrics
    order_total_value,
    items_in_order,
    avg_item_price_in_order,
    customer_lifetime_orders,
    customer_lifetime_value,
    product_total_quantity_sold,
    product_total_revenue,
    
    -- Metadata
    _operation,
    _event_timestamp_ms,
    _extracted_at,
    _processed_at
    
FROM order_metrics
ORDER BY ordered_at DESC, order_id, product_id 