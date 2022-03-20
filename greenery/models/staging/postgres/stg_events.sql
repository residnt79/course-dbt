
WITH source_data AS (
    SELECT *
    FROM {{ source('greenery','events') }}
)
, final AS (

    SELECT 
        event_id AS event_guid,
        session_id AS session_guid,
        user_id AS user_guid,
        page_url,
        created_at AS created_at_timestamp_utc,
        event_type,
        order_id AS order_guid,
        product_id AS product_guid,
        date_part('hour', created_at)::NUMERIC AS created_at_hour,
        substring(created_at::date::varchar, 1,10)::DATE AS created_at_date
    FROM source_data
)

SELECT *
FROM final