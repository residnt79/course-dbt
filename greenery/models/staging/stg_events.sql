{{
    config(
        materialized='table'
    )
}}

WITH source_data AS (
    SELECT 
        event_id AS event_guid,
        session_id AS session_guid,
        user_id AS user_guid,
        page_url,
        created_at AS created_at_timestamp,
        event_type,
        order_id AS order_guid,
        product_id AS product_guid
    FROM {{ source('tutorial','events') }}
)
, final AS (

    SELECT 
        *,
        date_part('hour', created_at_timestamp)::NUMERIC AS created_at_hour,
        substring(created_at_timestamp::date::varchar, 1,10)::DATE AS created_at_date
    FROM source_data
)

SELECT *
FROM final