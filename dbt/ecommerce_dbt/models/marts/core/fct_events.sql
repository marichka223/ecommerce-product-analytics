{{config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'event_pk',
    on_schema_change = 'sync_all_columns'
)}}

{% set lookback_days = var('lookback_days', 3)%}

WITH src AS (
    SELECT *
    FROM {{ ref('stg_clickstream_events')}}

    {% if is_incremental() %}
    WHERE ingested_at >=(
        SELECT COALESCE(MAX(ingested_at), TIMESTAMP '1900-01-01')
        FROM {{this}}
    ) - INTERVAL '{{ lookback_days }} days'
    {% endif %}
),

typed AS (
    SELECT 
        -- canonical event key
        coalesce(
            nullif(trim(event_id), ''),
            payload_hash
        ) AS event_pk,

        -- business time
        event_ts,
        CAST(event_ts AS DATE) AS event_date,

        -- received time
        ingested_at as received_ts,

        -- normalized fields
        nullif(lower(trim(event_type)), '') AS event_type,

        user_id,
        nullif(trim(user_session), '') AS user_session,

        nullif(trim(product_id), '') AS product_id,
        nullif(trim(category_id), '') AS category_id,
        nullif(trim(category_code), '') AS category_code,
        brand,
        try_cast(price AS DOUBLE) AS price,

        nullif(trim(source_system), '') AS source_system, 
        nullif(trim(schema_version), '') AS schema_version,
        nullif(trim(batch_id), '') AS batch_id,

        dt,
        filename,
        load_id,
        ingested_at,
        payload_hash
    
    FROM src

),

scored AS (
    SELECT 
        t.*,
        (CASE WHEN event_ts IS NOT NULL THEN 1 ELSE 0 END  +
        CASE WHEN user_id IS NOT NULL THEN 1 ELSE 0 END  +
        CASE WHEN event_type IS NOT NULL THEN 1 ELSE 0 END  +
        CASE WHEN user_session IS NOT NULL THEN 1 ELSE 0 END
        ) AS score
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
                    score DESC,
                    received_ts DESC
                    ) AS rn 
        FROM scored s
    ) x
    WHERE rn =1
)

SELECT
    event_pk,

    event_ts AS event_time,
    event_date,
    received_ts,

    event_type,
    user_id,
    user_session,

    product_id,
    category_id,
    category_code,
    brand,
    price,

    source_system,
    schema_version,
    batch_id,

    dt,
    filename,
    load_id,
    ingested_at,
    payload_hash

FROM dedup 
