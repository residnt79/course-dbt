WITH int_product_session AS (

    SELECT * FROM {{ ref('int_fact_product_session_agg') }}
)
, products as (

    SELECT * FROM {{ ref('stg_products') }}
)
, aggregation as (
    select
        product.product_name,
        sum(page_view) as views,
        sum(add_to_cart) as add_to_cart,
        sum(checkout) as checkout
    from int_product_session as ips 
    inner join products product 
    on ips.product_guid = product.product_guid
    group by 1
)
, final as (
    select 
        product_name,
        views,
        add_to_cart,
        checkout,
        round(checkout::numeric / views, 2) as product_conversion_rate,
        round((add_to_cart - checkout) / views::numeric,2) as unordered_rate
    from aggregation
)

SELECT *
FROM final