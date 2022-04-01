WITH int_sessions AS (

    SELECT * FROM {{ ref('int_sessions_snapshot') }}
)
, funnel as (
    SELECT 
        'Total Sessions' as funnel_step,
        SUM(CASE WHEN page_view > 0
                OR add_to_cart > 0
                OR checkout > 0 
                OR has_purchase = 1
            THEN 1 ELSE 0 END) as data_value
    FROM int_sessions

    UNION ALL

    SELECT
        'Add to Cart Sessions' as funnel_step,
        SUM(CASE WHEN add_to_cart > 0
                OR checkout > 0 
                OR has_purchase = 1
            THEN 1 ELSE 0 END) as data_value
    FROM int_sessions

    UNION ALL
    
    SELECT
        'Checkout Sessions' as funnel_step,
        SUM(CASE WHEN checkout > 0 
                OR has_purchase = 1
        THEN 1 ELSE 0 END) as data_value
    FROM int_sessions

    UNION ALL
    
    SELECT
        'Purchase Sessions' as funnel_step,
        SUM(has_purchase) as data_value
    FROM int_sessions
)
, prior_steps as (
    SELECT 
        funnel_step,
        data_value,
        lag(data_value) over () as prior_step,
        first_value(data_value) over () as first_step
    FROM funnel 
)
, final as (
    SELECT 
        funnel_step,
        data_value,
        prior_step,
        round(data_value / prior_step::numeric, 2) as prior_step_conversion_pct,
        (1 - round(data_value / prior_step::numeric, 2)) * 100 as prior_step_conversion_ppt,
        round(data_value / first_step::numeric, 2) as first_step_conversion_pct,
        (1 - round(data_value / first_step::numeric, 2)) * 100 as first_step_conversion_ppt
    FROM prior_steps
)

SELECT *
FROM final