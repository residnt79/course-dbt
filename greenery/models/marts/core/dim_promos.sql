with promos as (

    SELECT * FROM {{ ref('stg_promos') }}
)

select 
    promo_guid,
    discount_percent,
    status
from promos