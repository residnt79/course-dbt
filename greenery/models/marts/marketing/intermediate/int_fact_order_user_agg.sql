{{
  config(
    materialized='view'
  )
}}

with fact_orders as (

    SELECT * from {{ ref('fact_orders')}}

)

SELECT 
    user_guid,
    status,
    count(order_guid) as number_of_purchases,
    sum(order_cost_usd) as ltd_order_cost_usd,
    sum(shipping_cost_usd) as ltd_shipping_costs_usd,
    sum(order_total_usd) as ltd_order_total_usd
FROM fact_orders
{{ dbt_utils.group_by(n=2) }}

