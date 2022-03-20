with orders as (

    SELECT * from {{ ref('stg_orders') }}

)

SELECT
    order_guid,
    user_guid,
    promo_guid,
    address_guid,
    created_at_timestamp_utc,
    order_cost_usd,
    shipping_cost_usd,
    order_total_usd,
    tracking_guid,
    shipping_service,
    estimated_delivery_at_timestamp_utc,
    delivered_at_timestamp_utc,
    status,
    created_at_hour,
    created_at_date,
    seconds_to_delivery

FROM orders