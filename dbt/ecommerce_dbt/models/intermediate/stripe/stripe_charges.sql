{{config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'charge_id',
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
        provider_created_ts,
        event_time,
        event_type,
        ingested_at,
        raw_payload_json
    FROM {{ ref('stg_stripe_events') }}
    WHERE LOWER(event_type) LIKE 'charge.%'

),

filtered AS (

    SELECT s.*
    FROM src s
    CROSS JOIN last_run lr
    WHERE s.ingested_at > lr.max_received_ts - INTERVAL '{{ lookback_days }} days'

),

parsed AS (

    SELECT 
        -- canonical ids / timestamps
        json_extract_string(raw_payload_json, '$.data.object.id') AS charge_id,
        coalesce(event_time, provider_created_ts) AS occured_ts,
        ingested_at AS received_ts,

        -- core fields
        json_extract_string(raw_payload_json, '$.data.object.status') AS status,
        json_extract_string(raw_payload_json, '$.data.object.amount') AS amount_gross,
        json_extract_string(raw_payload_json, '$.data.object.currency') AS currency,

        -- relations
        json_extract_string(raw_payload_json, '$.data.object.payment_intent') AS payment_intent_id,

        -- balance transaction
        json_extract_string(raw_payload_json, '$.data.object.balance_transaction.fee') AS fee,
        json_extract_string(raw_payload_json, '$.data.object.balance_transaction.net') AS amount_net,

        event_type,
        provider_event_id,

        json_extract(raw_payload_json, '$.data.object') AS charge_json

    FROM filtered
    WHERE json_extract_string(raw_payload_json, '$.data.object.id') IS NOT NULL

),

cleaned AS (

    SELECT
        nullif(trim(charge_id), '') AS charge_id,
        occured_ts,
        received_ts,

        nullif(trim(status), '') AS status,
        try_cast(amount_gross AS BIGINT) /100 AS amount_gross,
        nullif(trim(currency), '') AS currency,

        nullif(trim(payment_intent_id), '') AS payment_intent_id,

        try_cast(fee AS BIGINT) /100 AS fee,
        try_cast(amount_net AS BIGINT) /100 AS amount_net,

        nullif(lower(trim(event_type)), '') AS event_type,
        nullif(trim(provider_event_id), '') AS provider_event_id,

        charge_json

    FROM parsed

),

scored AS (
    SELECT 
        c.*,
        (CASE WHEN occured_ts IS NOT NULL THEN 1 ELSE 0 END  +
        CASE WHEN status IS NOT NULL THEN 1 ELSE 0 END  +
        CASE WHEN amount_gross IS NOT NULL THEN 1 ELSE 0 END  +
        CASE WHEN currency IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN fee IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN amount_net IS NOT NULL THEN 1 ELSE 0 END
        ) AS score
    FROM cleaned c
    WHERE charge_id IS NOT NULL
),
dedup AS (
    SELECT *
    FROM (
        SELECT
            s.*,
            row_number() OVER(
                PARTITION BY charge_id
                ORDER BY
                    score DESC,
                    received_ts DESC
                    ) AS rn 
        FROM scored s
    ) x
    WHERE rn =1
)
SELECT 
    charge_id,
    occured_ts AS event_time,
    received_ts,

    status,
    amount_gross,
    currency,
    fee,
    amount_net,

    payment_intent_id,

    event_type,
    provider_event_id,

    charge_json
FROM dedup
