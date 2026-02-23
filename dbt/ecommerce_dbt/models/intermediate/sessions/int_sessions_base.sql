{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'session_id',
    on_schema_change = 'sync_all_columns'
) }}

{% set lookback_days = var('lookback_days', 3) %}

WITH src AS (

SELECT *
FROM {{ ref('fct_events') }}
WHERE user_session IS NOT NULL

{% if is_incremental() %}
  AND received_ts >= (
    SELECT coalesce(max(last_received_ts), TIMESTAMP '1900-01-01')
    FROM {{ this }}
  ) - interval '{{ lookback_days }} days'
{% endif %}
),

final AS (

SELECT
  user_session AS session_id,
  max(user_id) AS user_id,

  min(event_time) AS session_start_at,
  max(event_time) AS session_end_at,

  max(received_ts) AS last_received_ts,

  count(*) AS events_count,
  count(DISTINCT event_type) AS distinct_event_types_count,

  max(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) = 1 AS has_product_view,
  max(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) = 1 AS has_add_to_cart,
  max(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) = 1 AS has_purchase_event

FROM src
GROUP BY 1

)

SELECT
  session_id,
  user_id,

  session_start_at,
  session_end_at,

  CASE
    WHEN session_start_at IS NULL OR session_end_at IS NULL THEN NULL
    ELSE datediff('second', session_start_at, session_end_at)
  END AS session_duration_seconds,

  CAST(session_start_at AS DATE) AS session_date,

  events_count,
  distinct_event_types_count,

  (events_count = 1) AS is_bounce,

  has_product_view,
  has_add_to_cart,
  has_purchase_event,

  last_received_ts

FROM final