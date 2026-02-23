{{ config(
    materialized = 'table'
)}}

WITH daily AS(
    SELECT 
        activity_date,
        count(DISTINCT user_id) AS dau 
    FROM {{ ref('fct_user_daily_activity')}}
    WHERE is_active
    GROUP BY 1
),

rolling AS (
    SELECT 
        d.activity_date,
        d.dau,

        (SELECT count(DISTINCT user_id)
        FROM {{ ref('fct_user_daily_activity')}} f
        WHERE f.is_active
        AND f.activity_date BETWEEN d.activity_date - INTERVAL '6 days' AND d.activity_date
        ) AS wau,

        (SELECT count(DISTINCT user_id)
        FROM {{ ref('fct_user_daily_activity')}} f
        WHERE f.is_active
        AND f.activity_date BETWEEN d.activity_date - INTERVAL '29 days' AND d.activity_date
        ) AS mau
    
    FROM daily d
)

SELECT 
    activity_date,
    dau,
    wau,
    mau,
    wau * 1.0 / NULLIF(dau, 0) wau_dau_ratio,
    mau * 1.0 / NULLIF(dau, 0) mau_dau_ratio
FROM rolling