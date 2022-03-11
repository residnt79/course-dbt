{{
    config(
        materialized='table'
    )
}}

WITH source_data AS (
    SELECT
        order_id AS order_guid,
        user_id AS user_guid,
        promo_id AS promo_guid,
        address_id AS address_guid,
        created_at AS created_at_timestamp,
        order_cost,
        shipping_cost,
        order_total,
        tracking_id AS tracking_guid,
        shipping_service,
        estimated_delivery_at AS estimated_delivery_at_timestamp,
        delivered_at AS delivered_at_timestamp,
        status
    FROM {{ source('tutorial','orders') }}
)
, final AS (
    SELECT 
        *,
        date_part('hour', created_at_timestamp)::NUMERIC AS created_at_hour,
        substring(created_at_timestamp::date::varchar, 1,10)::DATE AS created_at_date,
        (extract(epoch from delivered_at_timestamp) - extract(epoch from created_at_timestamp))::NUMERIC AS seconds_to_delivery
    FROM source_data

)

SELECT *
FROM final