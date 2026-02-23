{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'payment_intent_id',
    on_schema_change = 'sync_all_columns'
)}}

{% set lookback_days = var('lookback_days', 3)%}

WITH last_run AS (

    {% if is_incremental() %}
        SELECT COALESCE(MAX(received_ts), TIMESTAMP '1900-01-01') AS max_received_ts
        FROM {{ this }}
    {% else %}
        SELECT TIMESTAMP '1900-01-01' AS max_received_ts
    {% endif %}

),

src AS (

    SELECT
        provider_event_id,
        event_time,
        event_type,
        received_ts,
        user_id,
        user_session,
        raw_payload_json
    FROM {{ ref('stripe_events_clean') }}
    WHERE LOWER(event_type) LIKE 'payment_intent.%'

),

filtered AS (

    SELECT s.*
    FROM src s
    CROSS JOIN last_run lr
    WHERE s.received_ts > lr.max_received_ts - INTERVAL '{{ lookback_days }} days'

),
parsed AS (
    SELECT 
        json_extract_string(raw_payload_json, '$.data.object.id') AS payment_intent_id,

        provider_event_id,
        lower(trim(event_type)) AS event_type,

        event_time,
        received_ts,

        raw_payload_json
    FROM filtered
)

SELECT
    provider_event_id,
    event_type,
    event_time,
    received_ts,

    payment_intent_id,

    'payment_intent_id is null' AS rejection_reason,
    raw_payload_json
FROM parsed
WHERE payment_intent_id IS NULL