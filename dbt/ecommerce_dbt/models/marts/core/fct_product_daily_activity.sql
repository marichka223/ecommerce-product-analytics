{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['product_id', 'activity_date'],
    on_schema_change = 'sync_all_columns'
) }}

{% set lookback_days = var('lookback_days', 3) %}

WITH events AS (

    SELECT
        product_id,
        CAST(event_time AS DATE) AS activity_date,

        COUNT(*) AS events_count,

        SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS views_count,
        SUM(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) AS add_to_cart_count,
        SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchase_events_count

    FROM {{ ref('fct_events') }}

    WHERE product_id IS NOT NULL

    {% if is_incremental() %}
    AND event_time >= (
        SELECT coalesce(MAX(activity_date), DATE '1900-01-01')
        FROM {{ this }}
    ) - INTERVAL '{{ lookback_days }} days'
    {% endif %}

    GROUP BY 1,2
),

payment_events AS (

    SELECT
        m.event_id,
        p.payment_intent_id,
        p.amount,
        p.refunded_amount,
        p.is_succeeded,
        p.is_refunded
    FROM {{ ref('map_payment_to_event') }} m
    JOIN {{ ref('fct_payments') }} p
      ON m.payment_intent_id = p.payment_intent_id
),

payment_products AS (

    SELECT
        e.product_id,
        CAST(e.event_time AS DATE) AS activity_date,

        COUNT(*) FILTER (WHERE p.is_succeeded) AS payments_count,
        SUM(CASE WHEN p.is_succeeded THEN p.amount ELSE 0 END) AS revenue_amount,
        SUM(CASE WHEN p.is_refunded THEN p.refunded_amount ELSE 0 END) AS refunded_amount

    FROM payment_events p
    JOIN {{ ref('fct_events') }} e
      ON p.event_id = e.event_pk

    WHERE e.product_id IS NOT NULL

    {% if is_incremental() %}
    AND e.event_time >= (
        SELECT coalesce(MAX(activity_date), DATE '1900-01-01')
        FROM {{ this }}
    ) - INTERVAL '{{ lookback_days }} days'
    {% endif %}

    GROUP BY 1,2
),

unioned AS (
    SELECT product_id, activity_date FROM events
    UNION
    SELECT product_id, activity_date FROM payment_products
),

final AS (

    SELECT
        u.product_id,
        u.activity_date,

        coalesce(e.events_count, 0) AS events_count,
        coalesce(e.views_count, 0) AS views_count,
        coalesce(e.add_to_cart_count, 0) AS add_to_cart_count,
        coalesce(e.purchase_events_count, 0) AS purchase_events_count,

        coalesce(p.payments_count, 0) AS payments_count,
        coalesce(p.revenue_amount, 0) AS revenue_amount,
        coalesce(p.refunded_amount, 0) AS refunded_amount,

        (coalesce(e.views_count, 0) > 0) AS had_view,
        (coalesce(p.payments_count, 0) > 0) AS had_payment,

        current_timestamp AS updated_at

    FROM unioned u
    LEFT JOIN events e
      ON u.product_id = e.product_id
     AND u.activity_date = e.activity_date
    LEFT JOIN payment_products p
      ON u.product_id = p.product_id
     AND u.activity_date = p.activity_date
)

SELECT * FROM final

