{{ config(
    materialized = 'table'
)}}

WITH base AS (
    SELECT
        event_id,
        metadata_json,
        keys_json,
        raw_payload_json,
        dt,
        filename,
        load_id,
        ingested_at,
        payload_hash,

        row_number() OVER(
            PARTITION BY payload_hash
            ORDER BY 
                ingested_at DESC,
                filename DESC
        ) AS rn
    FROM {{ source('landing', 'stripe_events_raw')}} 
),

src as (

    SELECT *
    FROM base
    WHERE rn=1
)

SELECT 
    json_extract_string(metadata_json, '$.event_id') AS  envelope_event_id,
    json_extract_string(metadata_json, '$.source_system') AS source_system,
    json_extract_string(metadata_json, '$.schema_version') AS schema_version,
    json_extract_string(metadata_json, '$.batch_id') AS batch_id,

    TRY_CAST(json_extract_string(metadata_json, '$.ingestion_timestamp') AS TIMESTAMP) AS envelope_timestamp,
    event_id AS provider_event_id,
    json_extract_string(raw_payload_json, '$.api_version') AS api_version,
    try_cast(json_extract(keys_json, '$.event_time') as timestamp) as event_time,

    try_cast(json_extract_string(keys_json, '$.user_id') as bigint) as user_id,
    json_extract_string(keys_json, '$.user_session') as user_session,
    cast(to_timestamp(try_cast(json_extract_string(raw_payload_json, '$.created') AS BIGINT)) as timestamp) AS provider_created_ts,
    json_extract_string(raw_payload_json, '$.type') as event_type,

    -- lineage
    dt,
    filename,
    load_id,
    ingested_at,
    payload_hash,

    -- raw
    metadata_json,
    keys_json,
    raw_payload_json

    from src


