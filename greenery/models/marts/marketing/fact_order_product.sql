with order_product as( 

    select * from {{ ref('int_fact_order_product_agg') }}
)

select 
    product_name,
    price_usd as unit_price_usd,
    units_sold,
    total_gross_usd,
    total_discount_usd,
    total_net_usd
from order_product