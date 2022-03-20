{% set event_type = dbt_utils.get_column_values(
    table=ref('int_fact_events_users_agg'),
    column='event_type'
) %}


WITH session_length AS (

    SELECT
        session_guid,
        user_guid,
        min(created_at_timestamp_utc) as first_event,
        max(created_at_timestamp_utc) as last_event
    FROM {{ ref('fact_events') }}
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

    SELECT * FROM {{ ref('dim_users') }}
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

        {% for event in event_type -%}
            SUM(CASE WHEN e.event_type = '{{ event }}' then event_type_count else 0 END ) AS {{ event }}
        {% if not loop.last -%}
            ,
        {%- endif %}
        {% endfor -%}
        
        
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
