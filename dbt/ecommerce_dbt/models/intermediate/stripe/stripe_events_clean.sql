{{ config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'event_pk',
        on_schema_change = 'sync_all_columns'
)}}

{% set lookback_days = var('lookback_days', 3) %}


WITH last_run AS (

    {% if is_incremental() %}
        SELECT COALESCE(MAX(ingested_at), TIMESTAMP '1900-01-01') AS max_ingested_at
        FROM {{ this }}
    {% else %}
        SELECT TIMESTAMP '1900-01-01' AS max_ingested_at
    {% endif %}

),

src AS (

    SELECT 
        envelope_event_id,
        source_system,
        schema_version,
        batch_id,
        envelope_timestamp,

        provider_event_id,
        api_version,
        provider_created_ts,

        user_id,
        user_session,
        event_time,
        event_type,

        filename,
        dt,
        ingested_at,
        payload_hash,

        metadata_json,
        keys_json,
        raw_payload_json
    FROM {{ ref('stg_stripe_events') }}

),

filtered AS (

    SELECT s.*
    FROM src s
    CROSS JOIN last_run lr
    WHERE s.ingested_at > lr.max_ingested_at - INTERVAL '{{ lookback_days }} days'

),

typed AS (

    SELECT
        -- Canonical event key (1 row per logical event)
        coalesce(
            nullif(trim(provider_event_id), ''),
            nullif(trim(envelope_event_id), ''),
            payload_hash) AS event_pk,

        -- Event occurred time (business time)
        coalesce(event_time, provider_created_ts) AS occurred_ts,
        CAST(coalesce(event_time, provider_created_ts) AS DATE) AS occurred_date,

        --Event received time (ingestion time)
        coalesce(ingested_at, envelope_timestamp) AS received_ts,

        lower(trim(event_type)) AS event_type,

        user_id,
        nullif(trim(user_session), '') AS user_session,

        nullif(trim(provider_event_id), '') AS provider_event_id,
        nullif(trim(envelope_event_id), '') AS envelope_event_id,
        nullif(trim(api_version), '') AS api_version,

        nullif(trim(source_system), '') AS source_system,
        nullif(trim(schema_version), '') AS schema_version,
        nullif(trim(batch_id), '') AS batch_id,

        dt,
        filename,
        ingested_at,
        payload_hash,

        raw_payload_json

    FROM filtered
),

scored AS (
    SELECT 
        t.*,
        CASE 
            WHEN provider_event_id IS NOT NULL THEN 3
            WHEN envelope_event_id IS NOT NULL THEN 2
            ELSE 1
        END AS id_quality,
        CASE WHEN occurred_ts IS NOT NULL THEN 1 ELSE 0 END AS has_occured_ts,
        CASE WHEN event_type IS NOT NULL THEN 1 ELSE 0 END AS has_event_type
    FROM typed t
    WHERE event_pk IS NOT NULL
),
dedup AS (
    SELECT *
    FROM (
        SELECT
            s.*,
            row_number() OVER(
                PARTITION BY event_pk
                ORDER BY
                    id_quality DESC,
                    has_occured_ts DESC,
                    has_event_type DESC,

                    received_ts DESC,
                    schema_version DESC,
                    batch_id DESC) AS rn 
        FROM scored s
    ) x
    WHERE rn =1
)

SELECT 
    event_pk,

    occurred_ts as event_time,
    occurred_date as event_date,

    received_ts,

    event_type,
    user_id,
    user_session,

    provider_event_id,
    envelope_event_id,
    api_version,

    source_system,
    schema_version,
    batch_id,

    dt,
    filename,
    ingested_at,
    payload_hash,

    raw_payload_json
FROM dedup

