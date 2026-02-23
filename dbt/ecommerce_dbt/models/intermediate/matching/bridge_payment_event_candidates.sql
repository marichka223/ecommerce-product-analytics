{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'payment_intent_id',
    on_schema_change = 'sync_all_columns'
)}}

{% set lookback_days = var('lookback_days', 3) %}
{% set match_window_minutes = var('match_window_minutes', 60) %}

WITH payments AS (
    SELECT
        payment_intent_id,
        payment_intent_event_ts AS payment_event_time,
        user_id AS payment_user_id,
        user_session AS payment_user_session,
        last_received_ts
    FROM {{ ref('fct_payments')}}

    {% if is_incremental() %}
    WHERE last_received_ts >= (
        SELECT coalesce(max(last_received_ts), TIMESTAMP '1900-01-01')
        FROM {{ this }}
    ) - INTERVAL '{{ lookback_days }} days'
    {% endif %}
),

payment_bounds AS (
    SELECT 
        min(payment_event_time) AS min_payment_time,
        max(payment_event_time) AS max_payment_time
    FROM payments
),

events AS (
    SELECT
        event_pk AS event_id,
        user_id,
        user_session,
        event_type,
        event_time,
        received_ts
    FROM {{ ref('fct_events')}}

    WHERE event_time BETWEEN
        (SELECT min_payment_time FROM payment_bounds) - INTERVAL '{{ match_window_minutes }} minutes' AND
        (SELECT max_payment_time FROM payment_bounds) + INTERVAL '{{ match_window_minutes }} minutes'
),


match_by_session AS (
    SELECT 
        p.payment_intent_id,
        p.payment_event_time,
        p.last_received_ts,

        e.event_id,
        e.event_time AS clickstream_time,
        e.received_ts AS clickstream_received_ts,
        e.event_type,

        e.user_id AS event_user_id,
        e.user_session AS event_user_session,

        CASE
            WHEN e.event_type = 'purchase' THEN 120
            ELSE 100
            END AS match_score,
        'user_session' AS matched_type 
    FROM payments p
    JOIN events e 
        ON p.payment_user_session = e.user_session
        AND p.payment_user_session IS NOT NULL
        AND e.event_time BETWEEN p.payment_event_time - INTERVAL '{{ match_window_minutes }} minutes' AND
        p.payment_event_time + INTERVAL '{{ match_window_minutes }} minutes'
),

-- fallback
match_by_user AS (
    SELECT 
        p.payment_intent_id,
        p.payment_event_time,
        p.last_received_ts,

        e.event_id,
        e.event_time AS clickstream_time,
        e.received_ts AS clickstream_received_ts,
        e.event_type,

        e.user_id AS event_user_id,
        e.user_session AS event_user_session,

        CASE
            WHEN e.event_type = 'purchase' THEN 70
            ELSE 50
            END AS match_score,
        'user_id' AS matched_type 
    FROM payments p
    JOIN events e 
        ON p.payment_user_id = e.user_id
        AND p.payment_user_id IS NOT NULL
        AND e.event_time BETWEEN p.payment_event_time - INTERVAL '{{ match_window_minutes }} minutes' AND
        p.payment_event_time + INTERVAL '{{ match_window_minutes }} minutes'
),

unioned AS (
    SELECT * FROM match_by_session
    UNION ALL
    SELECT * FROM match_by_user
),

final AS (
    SELECT
        u.*,
        date_diff('second', u.clickstream_time, u.payment_event_time) AS time_diff_seconds,

    FROM unioned u 

)

SELECT * FROM final
