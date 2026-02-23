{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'charge_id',
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
        json_extract_string(raw_payload_json, '$.data.object.id') AS charge_id,

        provider_event_id,
        lower(trim(event_type)) AS event_type,

        COALESCE(event_time, provider_created_ts) AS occured_ts,
        ingested_at AS received_ts,

        raw_payload_json
    FROM filtered
)

SELECT
    provider_event_id,
    event_type,
    occured_ts AS event_ts,
    received_ts,

    charge_id,

    'charge_id is null' AS rejection_reason,
    raw_payload_json
FROM parsed
WHERE charge_id IS NULL