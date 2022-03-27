with int_events as (

    SELECT * FROM {{ ref('int_fact_events_users_agg') }}
)
, sessions_with_purchase as(

    SELECT
        session_guid,
        event_type,
        order_guid
    FROM int_events
    WHERE event_type = 'checkout'
    AND order_guid IS NOT NULL
    GROUP BY 1,2,3
)
, total_sessions as (
    SELECT
        session_guid
    FROM int_events
    GROUP BY 1
)
, final as (

    SELECT 
        ttl.session_guid,
        CASE WHEN swp.order_guid IS NOT NULL THEN 1 ELSE 0 END AS has_purchase
    FROM total_sessions ttl 
    LEFT JOIN sessions_with_purchase swp 
        ON ttl.session_guid = swp.session_guid
)

SELECT *
FROM final