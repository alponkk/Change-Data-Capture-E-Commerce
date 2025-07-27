{{ config(materialized='view') }}

WITH source_data AS (
    SELECT raw_data
    FROM {{ source('raw_ecom_data', 'mongo_ecom_orders') }}
),

parsed_orders AS (
    SELECT
        -- Extract order ID from MongoDB ObjectId format
        JSONExtractString(raw_data, 'payload.after._id.$oid') AS order_id,
        
        -- Extract customer ID from MongoDB ObjectId format  
        JSONExtractString(raw_data, 'payload.after.customer_id.$oid') AS customer_id,
        
        -- Extract and convert order date to DateTime
        CASE 
            WHEN JSONExtractString(raw_data, 'payload.after.order_date') != ''
            THEN parseDateTime64BestEffort(JSONExtractString(raw_data, 'payload.after.order_date'))
            ELSE NULL
        END AS ordered_at,
        
        -- Extract order status
        JSONExtractString(raw_data, 'payload.after.status') AS order_status,
        
        -- Extract line items as JSON string (keep as raw JSON for now)
        JSONExtractRaw(raw_data, 'payload.after.line_items') AS line_items,
        
        -- Add metadata columns
        JSONExtractString(raw_data, 'payload.op') AS _operation,
        JSONExtractUInt64(raw_data, 'payload.ts_ms') AS _event_timestamp_ms,
        now() AS _extracted_at,
        
        -- Keep original raw data for debugging if needed
        raw_data AS _raw_data

    FROM source_data
    WHERE 
        -- Only include records where payload.after exists (not delete operations)
        JSONHas(raw_data, 'payload.after')
        AND JSONExtractString(raw_data, 'payload.after._id.$oid') != ''
)

SELECT 
    order_id,
    customer_id,
    ordered_at,
    order_status,
    line_items,
    _operation,
    _event_timestamp_ms,
    _extracted_at
    -- Uncomment the line below if you want to keep raw data for debugging
    -- , _raw_data
FROM parsed_orders
WHERE 
    -- Filter out any records with missing critical fields
    order_id IS NOT NULL 
    AND order_id != ''
    AND customer_id IS NOT NULL 
    AND customer_id != '' 