
WITH source_data AS (
    SELECT *
    FROM {{ source('greenery','promos') }}
)
, final AS (

    SELECT 
        promo_id AS promo_guid,
        ROUND(1.0 * discount / 100, 2) as discount_percent,
        status
    FROM source_data
)

SELECT *
FROM final