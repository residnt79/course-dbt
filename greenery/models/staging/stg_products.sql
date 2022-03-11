{{
    config(
        materialized='table'
    )
}}

WITH source_data AS (
    SELECT
        product_id AS product_guid,
        name,
        price,
        inventory
    FROM {{ source('tutorial','products') }}
)
, final AS (
    SELECT *
    FROM source_data
)

SELECT *
FROM final
