

WITH source_data AS (
    SELECT *
    FROM {{ source('greenery', 'addresses') }}
)
, final AS (
    SELECT
        address_id AS address_guid,
        address,
        zipcode,
        state,
        country
    FROM source_data
)

SELECT *
FROM final