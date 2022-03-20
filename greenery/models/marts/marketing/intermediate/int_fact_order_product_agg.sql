{{
  config(
    materialized='view'
  )
}}

with orders as (

    SELECT * FROM {{ ref('fact_orders') }}
)
, products as (

    SELECT * FROM {{ ref('dim_products') }}
)
, order_items as (

    SELECT * FROM {{ ref('stg_order_items') }}
)
, promos as (

    SELECT * from {{ ref('dim_promos') }}
)
, orders_prep as (

    SELECT
        o.order_guid,
        items.quantity,
        p.product_name,
        p.price_usd,
        coalesce(promo.discount_percent) as discount_percent,
        items.quantity * price_usd as gross_total
    FROM orders AS o 
    LEFT JOIN order_items AS items   
        ON o.order_guid = items.order_guid
    LEFT JOIN products AS p 
        on items.product_guid = p.product_guid
    LEFT JOIN promos as promo
        on o.promo_guid = promo.promo_guid

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