{{config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'refund_id',
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
        provider_created_ts,
        event_time,
        event_type,
        ingested_at,
        raw_payload_json
    FROM {{ ref('stg_stripe_events')}}
    WHERE lower(event_type) LIKE 'refund.%'

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
        json_extract_string(raw_payload_json, '$.data.object.id') AS refund_id,
        COALESCE(event_time, provider_created_ts) AS occured_ts,
        ingested_at AS received_ts,

        -- core fields
        json_extract_string(raw_payload_json, '$.data.object.status') AS status,
        json_extract_string(raw_payload_json, '$.data.object.amount') AS amount_gross,
        json_extract_string(raw_payload_json, '$.data.object.currency') AS currency,
        json_extract_string(raw_payload_json, '$.data.object.reason') AS reason,

        -- relations
        json_extract_string(raw_payload_json, '$.data.object.charge') AS charge_id,
        json_extract_string(raw_payload_json, '$.data.object.metadata.order_id') AS order_id,

        event_type,
        provider_event_id,

        json_extract(raw_payload_json, '$.data.object') AS refund_json

    FROM filtered
    WHERE json_extract_string(raw_payload_json, '$.data.object.id') IS NOT NULL

),

cleaned AS (

    SELECT
        nullif(trim(refund_id), '') AS refund_id,
        occured_ts,
        received_ts,

        nullif(trim(status), '') AS status,
        try_cast(amount_gross AS BIGINT) /100 AS amount_gross,
        nullif(trim(currency), '') AS currency,
        nullif(trim(reason), '') AS reason,

        nullif(trim(charge_id), '') AS charge_id,
        nullif(trim(order_id), '') AS order_id,
  
        nullif(lower(trim(event_type)), '') AS event_type,
        nullif(trim(provider_event_id), '') AS provider_event_id,

        refund_json

    FROM parsed

),

dedup AS (
    SELECT *
    FROM (
        SELECT
            c.*,
            row_number() OVER(PARTITION BY refund_id
            ORDER BY received_ts, occured_ts) AS rn
        FROM cleaned c
    ) x
    WHERE rn =1
)

SELECT 
    refund_id,
    occured_ts AS event_time,
    received_ts,

    status,
    amount_gross AS amount_refunded,
    currency,
    reason,

    charge_id,
    order_id,

    event_type,
    provider_event_id,

    refund_json
FROM dedup
