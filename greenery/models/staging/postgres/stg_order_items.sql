
WITH source_data AS (
    SELECT *
    FROM {{ source('greenery','order_items') }}
)
, final AS (

    SELECT 
        order_id AS order_guid,
        product_id AS product_guid,
        quantity
    FROM source_data
)

SELECT *
FROM final