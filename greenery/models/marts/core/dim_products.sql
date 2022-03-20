
with products as (

    SELECT * FROM {{ ref('stg_products')}}

)

SELECT
    product_guid,
    name as product_name,
    price_usd,
    inventory
FROM products