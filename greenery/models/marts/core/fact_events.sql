WITH events AS (

    SELECT * FROM {{ ref('stg_events') }}
)

SELECT
    event_guid,
    session_guid,
    user_guid,
    page_url,
    created_at_timestamp_utc,
    event_type,
    order_guid,
    product_guid,
    created_at_hour,
    created_at_date
FROM events