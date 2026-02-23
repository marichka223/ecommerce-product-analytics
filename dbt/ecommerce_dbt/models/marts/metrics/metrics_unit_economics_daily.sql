{{ config(materialized = 'table' )}}

WITH base AS (
    SELECT 
        activity_date,

        count(DISTINCT CASE WHEN is_active THEN user_id END) AS active_users,
        count(DISTINCT CASE WHEN is_payer_day THEN user_id END) AS paying_users,

        sum(revenue_amount) AS revenue_amount
    FROM {{ ref('fct_user_daily_activity')}}
    GROUP BY 1
),

final AS (
    SELECT 
        activity_date,

        active_users,
        paying_users,
        revenue_amount,

        -- core metrics
        revenue_amount * 1.0 / nullif(active_users, 0) AS arpu,
        revenue_amount * 1.0 / nullif(paying_users, 0) AS arppu,
        paying_users * 1.0 / nullif(active_users, 0) AS payer_rate
    FROM base
)

SELECT * FROM final
ORDER BY activity_date