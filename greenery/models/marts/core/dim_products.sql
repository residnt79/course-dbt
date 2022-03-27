
with products as (

    SELECT * FROM {{ ref('stg_products')}}

)

SELECT
    product_guid,
    product_name,
    price_usd,
    inventory
FROM products