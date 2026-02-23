{{config(
    materialized = 'table'
)}}


WITH base AS(
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
    FROM {{source('landing', 'clickstream_events_raw')}} 
),

src AS(
    SELECT *
    FROM base
    WHERE rn=1
)

SELECT
    event_id,
    json_extract_string(metadata_json, '$.source_system') AS source_system,
    json_extract_string(metadata_json, '$.schema_version') AS schema_version,
    json_extract_string(metadata_json, '$.batch_id') AS batch_id,

    TRY_CAST(json_extract_string(metadata_json, '$.ingested_at') AS TIMESTAMP) AS envelope_timestamp,

    --user info
    json_extract_string(raw_payload_json, '$.user_id') AS user_id,
    json_extract_string(raw_payload_json, '$.user_session') AS user_session,

    TRY_CAST(json_extract_string(raw_payload_json, '$.event_time') AS TIMESTAMP) AS event_ts,
    json_extract_string(raw_payload_json, '$.event_type') AS event_type,

    --product info
    json_extract_string(raw_payload_json, '$.product_id') AS product_id,
    json_extract_string(raw_payload_json, '$.category_id') AS category_id,
    json_extract_string(raw_payload_json, '$.category_code') AS category_code,
    json_extract_string(raw_payload_json, '$.brand') AS brand,
    json_extract_string(raw_payload_json, '$.price') AS price,

    --lineage
    dt,
    filename,
    load_id,
    ingested_at,
    payload_hash,

    -- raw
    metadata_json,
    keys_json,
    raw_payload_json
FROM src



