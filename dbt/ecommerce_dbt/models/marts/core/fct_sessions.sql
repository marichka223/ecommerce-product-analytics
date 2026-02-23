{{ config(
    materialized = 'table'
) }}

WITH session_events AS (

    SELECT *
    FROM {{ ref('int_sessions_base') }}

),

session_payments AS (

SELECT
    m.session_id,

    count(DISTINCT m.payment_intent_id) AS payments_count,
    sum(p.amount) AS session_revenue,
    sum(p.refunded_amount) AS session_refunded_amount

FROM {{ ref('map_payment_to_session') }} m
JOIN {{ ref('fct_payments') }} p
    ON p.payment_intent_id = m.payment_intent_id
WHERE p.is_succeeded = TRUE
    AND m.session_id IS NOT NULL
    GROUP BY 1

),

final AS (

SELECT
    e.session_id,
    e.user_id,

    e.session_start_at,
    e.session_end_at,
    e.session_duration_seconds,
    e.session_date,

    e.events_count,
    e.distinct_event_types_count,
    e.is_bounce,

    e.has_product_view,
    e.has_add_to_cart,
    e.has_purchase_event,

    coalesce(p.payments_count, 0) AS payments_count,
    (coalesce(p.payments_count, 0) > 0) AS has_purchase,

    coalesce(p.session_revenue, 0) AS session_revenue,
    coalesce(p.session_refunded_amount, 0) AS session_refunded_amount

FROM session_events e
LEFT JOIN session_payments p
    ON e.session_id = p.session_id

)

SELECT *
FROM final