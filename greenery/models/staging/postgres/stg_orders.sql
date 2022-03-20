WITH source_data AS (
    SELECT *
    FROM {{ source('greenery','orders') }}
)
, final AS (
    SELECT 
        order_id AS order_guid,
        user_id AS user_guid,
        promo_id AS promo_guid,
        address_id AS address_guid,
        created_at AS created_at_timestamp_utc,
        order_cost AS order_cost_usd,
        shipping_cost AS shipping_cost_usd,
        order_total AS order_total_usd,
        tracking_id AS tracking_guid,
        shipping_service,
        estimated_delivery_at AS estimated_delivery_at_timestamp_utc,
        delivered_at AS delivered_at_timestamp_utc,
        status,
        date_part('hour', created_at)::NUMERIC AS created_at_hour,
        substring(created_at::date::varchar, 1,10)::DATE AS created_at_date,
        (extract(epoch from delivered_at) - extract(epoch from created_at))::NUMERIC AS seconds_to_delivery
    FROM source_data

)

SELECT *
FROM final