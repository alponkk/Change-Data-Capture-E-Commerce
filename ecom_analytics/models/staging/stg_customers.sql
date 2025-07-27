{{ config(materialized='view') }}

WITH source_data AS (
    SELECT raw_data
    FROM {{ source('raw_ecom_data', 'mongo_ecom_customers') }}
),

parsed_customers AS (
    SELECT
        -- Extract customer ID from MongoDB ObjectId format
        JSONExtractString(raw_data, 'payload.after._id.$oid') AS customer_id,
        
        -- Extract customer information
        JSONExtractString(raw_data, 'payload.after.first_name') AS first_name,
        JSONExtractString(raw_data, 'payload.after.last_name') AS last_name,
        JSONExtractString(raw_data, 'payload.after.email') AS email,
        
        -- Extract and convert registration date
        CASE 
            WHEN JSONExtractString(raw_data, 'payload.after.registration_date') != ''
            THEN parseDateTime64BestEffort(JSONExtractString(raw_data, 'payload.after.registration_date'))
            ELSE NULL
        END AS registered_at,
        
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
    customer_id,
    first_name,
    last_name,
    email,
    registered_at,
    _operation,
    _event_timestamp_ms,
    _extracted_at
FROM parsed_customers
WHERE 
    -- Filter out any records with missing critical fields
    customer_id IS NOT NULL 
    AND customer_id != ''
    AND email IS NOT NULL 
    AND email != '' 