{{
    config(
        materialized='table'
    )
}}

WITH source_data AS (
    SELECT
        order_id AS order_guid,
        product_id AS product_guid,
        quantity
    FROM {{ source('tutorial','order_items') }}
)
, final AS (

    SELECT *
    FROM source_data
)

SELECT *
FROM final