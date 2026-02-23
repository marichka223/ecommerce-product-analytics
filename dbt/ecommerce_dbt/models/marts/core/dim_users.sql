{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'user_id',
    on_schema_change = 'sync_all_columns'
)}}

{% set lookback_days = var('lookback_days', 3) %}

WITH base AS(

    SELECT 
        user_id,
        min(session_start_at) AS first_session_at,
        max(session_start_at) AS last_session_at
    FROM {{ref('fct_sessions')}}
    GROUP BY 1
),

payments AS (

    SELECT
        user_id,
        min(payment_intent_event_ts) AS first_purchase_at,
        max(payment_intent_event_ts) AS last_purchase_at,
        sum(CASE WHEN is_succeeded THEN amount END) AS lifetime_revenue
    FROM {{ref('fct_payments')}}
        GROUP BY 1
),

final AS(

    SELECT 
        b.user_id,

        b.first_session_at,
        CAST(b.first_session_at AS DATE) AS first_seen_date,
        b.last_session_at,

        p.first_purchase_at,
        p.last_purchase_at,
        coalesce(p.lifetime_revenue, 0) AS lifetime_revenue,

        (p.first_purchase_at IS NOT NULL) AS is_payer,

        CASE
            WHEN p.first_purchase_at IS NOT NULL THEN TRUE
            ELSE FALSE 
            END AS user_type,

            CURRENT_TIMESTAMP AS updated_at

    FROM base b 
    LEFT JOIN payments p 
        ON p.user_id = b.user_id
)
SELECT * FROM final