
with users as (
    select *
    from {{ ref('int_users') }}
)
, final as (

    select 
        user_guid,
        first_name,
        last_name,
        full_name,
        email,
        phone_number,
        address,
        zipcode,
        state,
        country
    from users as usr
)

select *
from final