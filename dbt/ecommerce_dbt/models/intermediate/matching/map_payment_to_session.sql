{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'payment_intent_id',
    on_schema_change = 'sync_all_columns'
) }}

{% set lookback_days = var('lookback_days', 3) %}

WITH map AS (

    SELECT *
    FROM {{ ref('map_payment_to_event')}}

    {% if is_incremental() %}
    WHERE last_received_ts >= (
        SELECT coalesce(max(last_received_ts), TIMESTAMP '1900-01-01')
        FROM {{ this }}
    ) - INTERVAL '{{ lookback_days}} days'
    {% endif %}
),

sessions AS (

    SELECT 
        session_id,
        user_id AS session_user_id,
        session_start_at,
        session_end_at
    FROM {{ ref('int_sessions_base') }}
),

final AS (

    SELECT 
        m.payment_intent_id,

        -- canonical session key
        s.session_id,

        CASE WHEN s.session_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_session_found,
        CASE WHEN s.session_user_id IS NULL OR m.event_user_id IS NULL THEN NULL
            ELSE (m.event_user_id = s.session_user_id)
            END AS is_user_consistent,

        m.event_id,
        m.event_type,
        m.event_user_id,
        m.event_user_session,
        m.matched_type,
        m.match_score,
        m.payment_event_time,
        m.clickstream_time,
        m.time_diff_seconds,

        m.last_received_ts
    FROM map m 
    LEFT JOIN sessions s 
        ON s.session_id = m.event_user_session
)
SELECT * FROM final