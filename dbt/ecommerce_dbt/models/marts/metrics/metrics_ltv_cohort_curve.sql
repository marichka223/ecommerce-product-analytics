{{config (materialized = 'table')}}

WITH cohorts AS (
    SELECT 
        user_id,
        first_seen_date AS cohort_date
    FROM {{ref('dim_users')}}
),

cohort_sizes AS (
    SELECT
        cohort_date,
        count(DISTINCT user_id) AS cohort_size
    FROM cohorts
    GROUP BY 1
),

daily_rev AS (
    SELECT 
        user_id,
        activity_date,
        revenue_amount - refunded_amount AS revenue_amount
    FROM {{ ref('fct_user_daily_activity')}}
),

rev_by_age AS (
    SELECT 
        c.cohort_date,
        datediff('day', c.cohort_date, dr.activity_date) AS cohort_age_days,
        sum(dr.revenue_amount) AS revenue_amount
    FROM cohorts c
    JOIN daily_rev dr
        ON c.user_id = dr.user_id
        AND dr.activity_date >=c.cohort_date
    GROUP BY 1,2
),

cohort_calendar AS (
    SELECT
        cs.cohort_date,
        gs.cohort_age_days,
        cs.cohort_size
    FROM cohort_sizes cs
    CROSS JOIN (
        SELECT generate_series AS cohort_age_days
        FROM generate_series(0,30)
    ) gs
),

curve AS (
    SELECT 
        cal.cohort_date,
        cal.cohort_age_days,
        cal.cohort_size,
        coalesce(r.revenue_amount, 0) AS revenue_amount,

        sum(coalesce(r.revenue_amount, 0)) OVER(
            PARTITION BY r.cohort_date
            ORDER BY r.cohort_age_days
            ROWS BETWEEN unbounded preceding AND current row
        ) AS cumulative_revenue
    FROM cohort_calendar cal
    LEFT JOIN rev_by_age r
        ON cal.cohort_date = r.cohort_date
       AND cal.cohort_age_days = r.cohort_age_days
    WHERE cal.cohort_date < '2019-11-01'
),

final AS (
    SELECT
        cohort_date,
        cohort_age_days,
        cohort_size,

        revenue_amount,
        cumulative_revenue,

        cumulative_revenue * 1.0 / nullif(cohort_size, 0) AS ltv 
    FROM curve
)

SELECT * FROM final
ORDER BY cohort_date, cohort_age_days