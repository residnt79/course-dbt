{{
    config(
        materialized='table'
    )
}}

WITH source_data AS (
    SELECT
        promo_id AS promo_guid,
        discount,
        status
    FROM {{ source('tutorial','promos') }}
)
, final AS (

    SELECT *
    FROM source_data
)

SELECT *
FROM final