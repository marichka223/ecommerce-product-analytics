{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'product_id',
    on_schema_change = 'sync_all_columns'
)}}

{% set lookback_days = var('lookback_days', 3) %}

WITH src AS
 (
    SELECT
        product_id,
        category_id,
        category_code,
        brand,
        price,
        event_time,
        received_ts
    FROM {{ ref('fct_events')}}
    WHERE product_id iS NOT NULL

    {% if is_incremental() %}
    AND received_ts >= (
        SELECT coalesce(max(last_received_ts), TIMESTAMP '1900-01-01')
        FROM {{ this}}
    ) - INTERVAL '{{ lookback_days }} days'
    {% endif %}

),

ranked AS (
    SELECT 
        s.*,
        row_number() OVER(PARTITION BY product_id
            ORDER BY event_time DESC, received_ts DESC) AS rn_last
    FROM src s
),

agg AS (

    SELECT
        product_id,

        -- lifecycle timestamps of the product
        min(event_time) AS first_seen_at,
        max(event_time) AS last_seen_at,

        max(received_ts) AS last_received_ts,
        count(*) AS events_count

    FROM src
    group by 1

),

last_attrs AS (
    SELECT
        product_id,
        brand AS last_brand,
        category_id AS last_category_id,
        category_code AS last_category_code,
        price AS last_price,
    FROM ranked
    WHERE rn_last = 1
)

SELECT
    a.product_id,

    la.last_brand AS brand,
    la.last_category_id AS category_id,
    la.last_category_code AS category_code,
    la.last_price AS price,

    a.first_seen_at,
    a.last_seen_at,
    a.events_count,
    a.last_received_ts
FROM agg a 
LEFT JOIN last_attrs la ON la.product_id = a.product_id
