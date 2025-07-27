{{ config(materialized='view') }}

WITH source_data AS (
    SELECT raw_data
    FROM {{ source('raw_ecom_data', 'mongo_ecom_products') }}
),

parsed_products AS (
    SELECT
        -- Extract product ID from MongoDB ObjectId format
        JSONExtractString(raw_data, 'payload.after._id.$oid') AS product_id,
        
        -- Extract product information
        JSONExtractString(raw_data, 'payload.after.name') AS product_name,
        
        -- Extract and convert price
        CASE 
            WHEN JSONExtractString(raw_data, 'payload.after.price') != ''
            THEN toFloat64OrZero(JSONExtractString(raw_data, 'payload.after.price'))
            ELSE 0.0
        END AS price,
        
        -- Extract stock quantity
        CASE 
            WHEN JSONExtractString(raw_data, 'payload.after.stock_quantity') != ''
            THEN toInt32OrZero(JSONExtractString(raw_data, 'payload.after.stock_quantity'))
            ELSE 0
        END AS stock_quantity,
        
        -- Add metadata columns
        JSONExtractString(raw_data, 'payload.op') AS _operation,
        JSONExtractUInt64(raw_data, 'payload.ts_ms') AS _event_timestamp_ms,
        now() AS _extracted_at

    FROM source_data
    WHERE 
        -- Only include records where payload.after exists (not delete operations)
        JSONHas(raw_data, 'payload.after')
        AND JSONExtractString(raw_data, 'payload.after._id.$oid') != ''
)

SELECT 
    product_id,
    product_name,
    price,
    stock_quantity,
    _operation,
    _event_timestamp_ms,
    _extracted_at
FROM parsed_products
WHERE 
    -- Filter out any records with missing critical fields
    product_id IS NOT NULL 
    AND product_id != ''
    AND product_name IS NOT NULL 
    AND product_name != '' 