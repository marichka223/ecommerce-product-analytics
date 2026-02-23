{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'payment_intent_id',
    on_schema_change = 'sync_all_columns'
)}}

{% set lookback_days = var('lookback_days', 3) %}

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
        -- canonical ids / timestamps
        json_extract_string(raw_payload_json, '$.data.object.id') AS payment_intent_id,
        event_time,
        received_ts,

        -- core fields
        json_extract_string(raw_payload_json, '$.data.object.status') AS status,
        json_extract_string(raw_payload_json, '$.data.object.amount') AS amount_gross,
        json_extract_string(raw_payload_json, '$.data.object.currency') AS currency,

        -- relations
        json_extract_string(raw_payload_json, '$.data.object.latest_charge') AS latest_charge_id,

        -- metadata
        json_extract_string(raw_payload_json, '$.data.object.metadata.order_id') AS order_id,
        json_extract_string(raw_payload_json, '$.data.object.metadata.attempt_number') AS attempt_number,
        json_extract_string(raw_payload_json, '$.data.object.metadata.payment_method') AS payment_method,

        event_type,
        provider_event_id,

        -- user info
        user_id,
        user_session,

        json_extract(raw_payload_json, '$.data.object') AS payment_intent_json
    FROM filtered
    WHERE json_extract_string(raw_payload_json, '$.data.object.id') IS NOT NULL
),

cleaned AS (

    SELECT
        nullif(trim(payment_intent_id), '') AS payment_intent_id,
        event_time,
        received_ts,

        nullif(trim(status), '') AS status,
        try_cast(amount_gross AS BIGINT) /100 AS amount_gross,
        nullif(trim(currency), '') AS currency,

        nullif(trim(latest_charge_id), '') AS latest_charge_id,

        nullif(trim(order_id), '') AS order_id,
        try_cast(attempt_number AS INT) AS attempt_number,
        nullif(trim(payment_method), '') AS payment_method,

        nullif(lower(trim(event_type)), '') AS event_type,
        nullif(trim(provider_event_id), '') AS provider_event_id,

        user_id,
        user_session,

        payment_intent_json

    FROM parsed

),

scored AS (
    SELECT 
        c.*,
        (CASE WHEN event_time IS NOT NULL THEN 1 ELSE 0 END  +
        CASE WHEN status IS NOT NULL THEN 1 ELSE 0 END  +
        CASE WHEN amount_gross IS NOT NULL THEN 1 ELSE 0 END  +
        CASE WHEN currency IS NOT NULL THEN 1 ELSE 0 END
        ) AS score
    FROM cleaned c
    WHERE payment_intent_id IS NOT NULL
),

dedup AS (
    SELECT *
    FROM (
        SELECT
            s.*,
            row_number() OVER(
                PARTITION BY payment_intent_id
                ORDER BY
                    score DESC,
                    received_ts DESC
                    ) AS rn 
        FROM scored s
    ) x
    WHERE rn =1
)

SELECT 
    payment_intent_id,
    event_time,
    received_ts,

    status,
    amount_gross,
    currency,

    latest_charge_id,
    order_id,
    attempt_number,
    payment_method,

    event_type,
    provider_event_id,

    user_id,
    user_session,

    payment_intent_json

FROM dedup