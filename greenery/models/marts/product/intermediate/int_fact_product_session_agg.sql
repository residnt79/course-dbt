{{
  config(
    materialized='view'
  )
}}

{% set event_types = dbt_utils.get_column_values(
    table=ref('stg_events'),
    column='event_type'
) %}

WITH events as (

    SELECT * FROM {{ ref('stg_events') }}
)
, order_items as (

    SELECT * FROM {{ ref('stg_order_items') }}
)
, page_cart_events as (

    SELECT session_guid,
        product_guid,
        {{ pivot_for_loop(event_types, 'event_type', agg='SUM', comma = 'end', end_comma = 'no') }}

    FROM events
    where order_guid IS NULL
    group by 1,2
)
, product_purchased as (
    select 
        session_guid,
        1 as checkout,
        o.product_guid
    from events e 
    inner join order_items o
    on e.order_guid = o.order_guid
    where event_type = 'checkout'
)
, purchase_and_views as (
    select 
        pce.session_guid,
        pce.product_guid,
        page_view,
        add_to_cart,
        coalesce(purch.checkout,0) as checkout
    from page_cart_events pce 
    left join product_purchased purch
    on pce.session_guid = purch.session_guid
    and pce.product_guid = purch.product_guid
)

select *
from purchase_and_views