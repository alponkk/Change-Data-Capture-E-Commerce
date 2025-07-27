{{ config(materialized='table') }}

WITH latest_customers AS (
    SELECT 
        customer_id,
        first_name,
        last_name,
        email,
        registered_at,
        _operation,
        _event_timestamp_ms,
        _extracted_at,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _event_timestamp_ms DESC) AS rn
    FROM {{ ref('stg_customers') }}
    WHERE _operation IN ('r', 'c', 'u')  -- read, create, update operations only
),

customer_dimension AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        concat(first_name, ' ', last_name) AS full_name,
        email,
        registered_at,
        CASE 
            WHEN registered_at >= subtractDays(now(), 30) THEN 'New'
            WHEN registered_at >= subtractDays(now(), 365) THEN 'Recent'
            ELSE 'Established'
        END AS customer_segment,
        dateDiff('day', registered_at, now()) AS days_since_registration,
        _event_timestamp_ms,
        _extracted_at,
        now() AS _processed_at
    FROM latest_customers
    WHERE rn = 1  -- Get only the latest version of each customer
)

SELECT 
    customer_id,
    first_name,
    last_name,
    full_name,
    email,
    registered_at,
    customer_segment,
    days_since_registration,
    _event_timestamp_ms,
    _extracted_at,
    _processed_at
FROM customer_dimension
ORDER BY registered_at DESC 