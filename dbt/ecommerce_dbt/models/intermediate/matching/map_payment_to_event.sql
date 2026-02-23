{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'payment_intent_id'
)}}

{% set lookback_days = var('lookback_days', 3) %}

WITH last_run AS (

    {% if is_incremental() %}
        SELECT COALESCE(MAX(last_received_ts), TIMESTAMP '1900-01-01') AS max_last_received_ts
        FROM {{ this }}
    {% else %}
        SELECT TIMESTAMP '1900-01-01' AS max_last_received_ts
    {% endif %}

),

candidates AS (

    SELECT *
    FROM {{ ref('bridge_payment_event_candidates') }}

),

filtered AS (

    SELECT c.*
    FROM candidates c
    CROSS JOIN last_run lr
    WHERE c.last_received_ts >= lr.max_last_received_ts - INTERVAL '{{ lookback_days }} days'

),

ranked AS (

    SELECT 
        f.*,
        row_number() OVER(
            PARTITION BY payment_intent_id
            ORDER BY 
                f.match_score DESC,
                abs(f.time_diff_seconds) asc,
                f.clickstream_received_ts DESC
        ) AS rn 
    FROM filtered f
)

SELECT
    payment_intent_id,
    event_id,

    event_user_id,
    event_user_session,
    event_type,

    matched_type,
    match_score,

    payment_event_time,
    clickstream_time,
    time_diff_seconds,

    last_received_ts
FROM ranked
WHERE rn = 1
