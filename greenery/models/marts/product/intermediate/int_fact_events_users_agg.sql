{{
  config(
    materialized='view'
  )
}}

WITH events as (

    SELECT * from {{ ref('stg_events') }}
)

SELECT
    session_guid,
    user_guid,
    created_at_date,
    event_type,
    order_guid,
    count(event_type) as event_type_count
FROM events
{{ dbt_utils.group_by(n=5) }}