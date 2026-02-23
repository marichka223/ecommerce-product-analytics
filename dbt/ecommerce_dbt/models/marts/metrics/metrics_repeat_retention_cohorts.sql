 {{config(materialized = 'table')}}

 WITH payments AS (
    SELECT
        user_id,
        payment_intent_event_ts
    FROM {{ref('fct_payments')}}
    WHERE is_succeeded
 ),

 first_purchase AS(
    SELECT
        user_id,
        min(payment_intent_event_ts) AS first_purchase_ts
    FROM payments
    GROUP BY 1
 ),

 user_cohorts AS (
    SELECT 
        user_id,
        DATE_TRUNC('week', first_purchase_ts) AS cohort_week,
        first_purchase_ts
    FROM first_purchase
 ),


first_repeat AS (
    SELECT
        uc.user_id,
        uc.cohort_week,
        MIN(p.payment_intent_event_ts) AS second_purchase_ts
    FROM user_cohorts uc
    JOIN payments p
        ON p.user_id = uc.user_id
       AND p.payment_intent_event_ts > uc.first_purchase_ts
    GROUP BY 1,2
),

repeat_age AS (
    SELECT
        user_id,
        cohort_week,
        DATEDIFF('day', first_purchase_ts, second_purchase_ts) AS days_to_2nd
    FROM (
        SELECT
            uc.user_id,
            uc.cohort_week,
            uc.first_purchase_ts,
            fr.second_purchase_ts
        FROM user_cohorts uc
        LEFT JOIN first_repeat fr
            ON fr.user_id = uc.user_id
           AND fr.cohort_week = uc.cohort_week)
),

 cohort_sizes AS (
    SELECT
        cohort_week,
        count(DISTINCT user_id) AS cohort_size
    FROM
        user_cohorts
    GROUP BY 1
 ),

horizons AS (
    SELECT 1 AS horizon_day UNION ALL
    SELECT 7 UNION ALL
    SELECT 14 UNION ALL
    SELECT 21 UNION ALL
    SELECT 30 UNION ALL
    SELECT 60
),

final AS (
    SELECT
        cs.cohort_week,
        h.horizon_day AS cohort_age_days,
        COUNT(DISTINCT CASE WHEN ra.days_to_2nd <= h.horizon_day THEN ra.user_id END) AS retained_users,
        cs.cohort_size AS cohort_size,
        COUNT(DISTINCT CASE WHEN ra.days_to_2nd <= h.horizon_day THEN ra.user_id END) * 1.0 / NULLIF(cs.cohort_size, 0) AS retention_rate
    FROM cohort_sizes cs
    CROSS JOIN horizons h
    LEFT JOIN repeat_age ra
        ON ra.cohort_week = cs.cohort_week
    GROUP BY 1,2,4
)

SELECT * FROM final