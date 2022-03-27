{{
  config(
    materialized='view'
  )
}}

with orders as (

    SELECT * FROM {{ ref('stg_orders') }}
)
, products as (

    SELECT * FROM {{ ref('stg_products') }}
)
, order_items as (

    SELECT * FROM {{ ref('stg_order_items') }}
)
, promos as (

    SELECT * from {{ ref('stg_promos') }}
)
, orders_prep as (

    SELECT
        orders.order_guid,
        items.quantity,
        product.product_name,
        product.price_usd,
        coalesce(promo.discount_percent) as discount_percent,
        items.quantity * price_usd as gross_total
    FROM orders AS orders
    LEFT JOIN order_items AS items   
        ON orders.order_guid = items.order_guid
    LEFT JOIN products AS product 
        on items.product_guid = product.product_guid
    LEFT JOIN promos as promo
        on orders.promo_guid = promo.promo_guid

)
, pricing as (

    SELECT
        order_guid,
        quantity,
        product_name,
        price_usd,
        discount_percent,
        gross_total,
        gross_total * discount_percent as discount_usd,
        gross_total - (gross_total * discount_percent) as net_total
    from orders_prep
)
, final as (

    SELECT 
        product_name,
        price_usd,
        sum(quantity) as units_sold,
        sum(gross_total) as total_gross_usd,
        sum(discount_usd) as total_discount_usd,
        sum(net_total) as total_net_usd
    FROM pricing
    group by 1,2
)

SELECT *
FROM final