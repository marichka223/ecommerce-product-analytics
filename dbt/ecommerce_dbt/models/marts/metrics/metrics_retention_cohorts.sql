 {{config(materialized = 'table')}}

 WITH user_cohorts AS (
    SELECT 
        user_id,
        first_seen_date AS cohort_date,
    FROM {{ ref('dim_users')}}
 ),

 activity AS (
    SELECT
        user_id,
        activity_date,
    FROM {{ref('fct_user_daily_activity')}}
    WHERE is_active
 ),

 joined AS (
    SELECT 
        uc.user_id,
        uc.cohort_date,
        activity_date,
        DATEDIFF('day', uc.cohort_date, a.activity_date) AS cohort_age_days
    FROM user_cohorts uc
    JOIN activity a 
        ON a.user_id = uc.user_id
        AND a.activity_date >= uc.cohort_date
 ),

 cohort_sizes AS (
    SELECT
        cohort_date,
        count(DISTINCT user_id) AS cohort_sizes
    FROM
        user_cohorts
    GROUP BY 1
 ),

 retention AS (
    SELECT
        cohort_date,
        cohort_age_days,
        count(DISTINCT user_id) AS retained_users
    FROM joined 
    GROUP BY 1,2
 ),
 
 final AS (
    SELECT
        r.cohort_date,
        r.cohort_age_days,

        r.retained_users,
        cs.cohort_sizes,

        r.retained_users * 1.0 / NULLIF(cs.cohort_sizes, 0) AS retention_rate
    FROM retention r 
    JOIN cohort_sizes cs 
        ON r.cohort_date = cs.cohort_date
 )
 SELECT * FROM final
 