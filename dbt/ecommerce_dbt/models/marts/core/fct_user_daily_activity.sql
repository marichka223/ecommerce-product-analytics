{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['user_id', 'activity_date'],
    on_schema_change = 'sync_all_columns'
) }}
{% set lookback_days = var('lookback_days', 3) %}

WITH last_run AS (

    {% if is_incremental() %}
        SELECT COALESCE(MAX(activity_date), DATE '1900-01-01') AS max_activity_date
        FROM {{ this }}
    {% else %}
        SELECT DATE '1900-01-01' AS max_activity_date
    {% endif %}

),

sessions AS (

    SELECT 
        user_id,
        session_date AS activity_date,
        COUNT(*) AS sessions_count,
        SUM(events_count) AS events_count
    FROM {{ ref('fct_sessions') }} fs
    CROSS JOIN last_run lr
    WHERE fs.session_date >= lr.max_activity_date - INTERVAL '{{ lookback_days }} days'
    GROUP BY 1, 2

),

payments AS (

    SELECT
        user_id,
        CAST(payment_intent_event_ts AS DATE) AS activity_date,
        SUM(CASE WHEN is_succeeded THEN 1 ELSE 0 END) AS payments_count,
        SUM(CASE WHEN is_succeeded THEN amount ELSE 0 END) AS revenue_amount,
        SUM(CASE WHEN is_refunded THEN refunded_amount ELSE 0 END) AS refunded_amount
    FROM {{ ref('fct_payments') }} fp
    CROSS JOIN last_run lr
    WHERE CAST(fp.payment_intent_event_ts AS DATE) >= lr.max_activity_date - INTERVAL '{{ lookback_days }} days'
    GROUP BY 1, 2
),

unioned_dates AS (
    SELECT user_id, activity_date FROM sessions
    UNION
    SELECT user_id, activity_date FROM payments
),

final AS (

    SELECT 
        u.user_id,
        u.activity_date,
        date_trunc('week', u.activity_date) AS week,

        coalesce(s.sessions_count, 0) AS sessions_count,
        coalesce(s.events_count, 0) AS events_count,

        coalesce(p.payments_count, 0) as payments_count,
        coalesce(p.revenue_amount, 0) as revenue_amount,
        coalesce(p.refunded_amount, 0) as refunded_amount,

        (coalesce(s.sessions_count,0) > 0) as is_active,
        (coalesce(p.payments_count,0) > 0) as is_payer_day,

        current_timestamp as updated_at
    FROM unioned_dates u 
    LEFT JOIN sessions s 
        ON s.user_id = u.user_id
        AND s.activity_date = u.activity_date
    LEFT JOIN payments p  
        ON p.user_id = u.user_id
        AND p.activity_date = u.activity_date
)

SELECT * FROM final


