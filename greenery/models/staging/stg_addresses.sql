
{{
    config(
        materialized='table'
    )
}}

WITH source_data AS (
    SELECT
        address_id AS address_guid,
        address,
        zipcode,
        state,
        country
    FROM {{ source('tutorial', 'addresses') }}
)
, final AS (
    SELECT *
    FROM source_data
)

SELECT *
FROM final