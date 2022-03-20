
WITH source_data AS (
    SELECT *
    FROM {{ source('greenery','users') }}
)
, final as (

    SELECT 
        user_id as user_guid,
        first_name,
        last_name,
        email,
        phone_number,
        created_at as created_at_timestamp_utc,
        updated_at,
        address_id as address_guid
    FROM source_data
)

SELECT *
FROM final