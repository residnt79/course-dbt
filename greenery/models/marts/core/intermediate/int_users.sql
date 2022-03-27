
{{
  config(
    materialized='view'
  )
}}

with users as (
    select *
    from {{ ref('stg_users') }}
)
, addresses as (
    SELECT *
    FROM {{ ref('stg_addresses') }}
)
, final as (

    select 
        user_guid,
        first_name,
        last_name,
        first_name || ' ' || last_name as full_name,
        email,
        phone_number,
        address,
        zipcode,
        state,
        country
    from users as usr
    left join addresses as addr
    on usr.address_guid = addr.address_guid
)

select *
from final