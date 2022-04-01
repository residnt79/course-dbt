{{
  config(
    materialized='view'
  )
}}

{% set event_type = dbt_utils.get_column_values(
    table=ref('stg_events'),
    column='event_type'
) %}

with events as (

    SELECT * from {{ ref('stg_events') }}
)
, event_pivot as (

    SELECT 
        session_guid,
        
        {{ pivot_for_loop(
            column_array = event_type,
            column = 'event_type', 
            agg='SUM', 
            comma = 'end', 
            end_comma = 'yes' 
        ) }}

        MAX(CASE WHEN order_guid IS NOT NULL THEN 1 ELSE 0 END) AS has_purchase,
        MAX(created_at_timestamp_utc) AS created_at_timestamp_utc
    FROM events
    GROUP BY 1
)

SELECT *
FROM event_pivot
