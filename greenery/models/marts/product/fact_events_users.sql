{% set event_types = dbt_utils.get_column_values(
    table=ref('int_fact_events_users_agg'),
    column='event_type'
) %}


WITH session_length AS (

    SELECT
        session_guid,
        user_guid,
        min(created_at_timestamp_utc) as first_event,
        max(created_at_timestamp_utc) as last_event
    FROM {{ ref('stg_events') }}
    group by 1,2
)
, session_length_seconds AS (

    SELECT
        session_guid,
        user_guid,
        first_event,
        last_event,
        (extract(epoch from last_event) - extract(epoch from first_event))::NUMERIC AS session_length_seconds
    FROM session_length
)
, int_events AS (

    SELECT * FROM {{ ref('int_fact_events_users_agg') }}
)
, users AS (

    SELECT * FROM {{ ref('int_users') }}
)
, final AS (

    SELECT 
        e.session_guid,
        e.user_guid,
        usr.full_name,
        usr.email,
        usr.state,
        usr.country,
        sl.first_event,
        sl.last_event,
        session_length_seconds / 60 AS session_length_minutes,

        {{ pivot_for_loop(
            column_array = event_types,
            column = 'e.event_type',
            then_column = 'event_type_count', 
            agg = 'SUM', 
            comma = 'end', 
            end_comma = 'yes',
        ) }}


        MAX(CASE WHEN e.order_guid IS NOT NULL THEN 1 ELSE 0 END) as has_purchase
        
    FROM int_events e 
    LEFT JOIN users usr
        ON e.user_guid = usr.user_guid
    LEFT JOIN session_length_seconds sl 
        on e.session_guid = sl.session_guid
        AND e.user_guid = sl.user_guid
    {{ dbt_utils.group_by(n=9) }}

)

SELECT *
FROM final
