{{
    config(
        materialized='table'
    )
}}

WITH source_data AS (
    SELECT
        user_id as user_guid,
        first_name,
        last_name,
        email,
        phone_number,
        created_at as created_at_timestamp,
        updated_at,
        address_id as address_guid
    FROM {{ source('tutorial','users') }}
)
, final as (

    SELECT *
    FROM source_data
)

SELECT *
FROM final