# Week1 Questions:

## How many users do we have?

Tests on the user table ensures uniqueness of the user_guid so `count(distinct)` is not needed.

~~~~sql
    SELECT COUNT(user_guid)
    FROM dbt_jason_s.stg_users
~~~~
**Answer:** 130

## On average, how many orders do we receive per hour?
In the staging table I extracted the date and hour from the created_at time_stamp to reduce additional logic being pushed into the sql query.

~~~~sql
    WITH orders_per_hour AS (
        SELECT 
            created_at_date,
            created_at_hour,
            COUNT(order_guid) order_count
        FROM dbt_jason_s.stg_orders
        GROUP BY 1,2
    )

    SELECT ROUND(AVG(order_count), 2)
    FROM orders_per_hour
~~~~
**Answer:** 7.52

## On average, how long does an order take from being placed to being delivered?

Created seconds_to_delivery in stg_orders to allow analysts to define the aggregation timeframe. Since we are dealing with sessions, I rounded to whole numbers as a user cannot have a partial session.

~~~~sql
SELECT ROUND(AVG(seconds_to_delivery / 3600), 1) AS average_delivery_hours
FROM dbt_jason_s.stg_orders
~~~~
**Answer:** 93.4 Hours

## How many users have only made one purchase? Two purchases? Three+ purchases?

~~~~sql
WITH customer_orders AS (
  SELECT user_guid,
    COUNT(order_guid) AS order_count
  FROM dbt_jason_s.stg_orders
  GROUP BY 1
)

SELECT 
  CASE WHEN order_count >= 3 THEN '3+ Purchases'
    WHEN order_count = 1 THEN order_count::TEXT || ' Purchase'
    ELSE order_count::TEXT || ' Purchases' END AS order_count,
  COUNT(user_guid) AS customer_count
FROM customer_orders
GROUP BY 1
ORDER BY order_count
~~~~
**Answer:**

| Number of Purchases | User Count|
| :----- | :-----: |
|1 Purchase | 25 |
|2 Purchases| 28 |
|3+ Purchases| 71 |

## On average, how many unique sessions do we have per hour?
In the staging table I extracted the date and hour from the created_at time_stamp to reduce additional logic being pushed into the sql query.


~~~~sql
WITH unique_sessions AS (
SELECT created_at_date,
  created_at_hour,
  COUNT(DISTINCT session_guid) AS session_count
FROM dbt_jason_s.stg_events
GROUP BY 1,2
)

SELECT round(avg(session_count)) AS average_unique_sessions
FROM unique_sessions
~~~~
**Answer:** 16
