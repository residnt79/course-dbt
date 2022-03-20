{% set statuses = dbt_utils.get_column_values(
    table=ref('int_fact_order_user_agg'),
    column='status'
) %}

with user_orders_agg as (

    SELECT * from {{ ref('int_fact_order_user_agg')}}
)
, orders_pivot as (

    SELECT 
        user_guid,
        
        {% for status in statuses -%}
        sum(case when status = '{{status}}' then number_of_purchases else 0 end) as {{status}}_orders,
        {% endfor -%}

        sum(number_of_purchases) as number_of_purchases,
        sum(ltd_order_cost_usd) as ltd_order_cost_usd,
        sum(ltd_shipping_costs_usd) as ltd_shipping_cost_usd,
        sum(ltd_order_total_usd) as ltd_order_total_usd
    FROM user_orders_agg 
    group by 1
)
, users as (

    SELECT * from {{ ref('dim_users') }}
)

SELECT
    op.user_guid,
    usr.full_name,
    usr.country,
    usr.state,
    CASE WHEN op.number_of_purchases > 1 then 1 else 0 end as is_repeat_spender,
    op.number_of_purchases,
    {% for status in statuses -%}
        op.{{ status }}_orders,
    {% endfor -%}
    op.ltd_order_cost_usd,
    op.ltd_shipping_cost_usd,
    op.ltd_order_total_usd

FROM orders_pivot as op
inner join users as usr
on op.user_guid = usr.user_guid