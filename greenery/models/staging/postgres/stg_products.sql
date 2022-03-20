
WITH source_data AS (
    SELECT *
    FROM {{ source('greenery','products') }}
)
, final AS (
    SELECT 
        product_id AS product_guid,
        name,
        price as price_usd,
        inventory
    FROM source_data
)

SELECT *
FROM final
