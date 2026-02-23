{{config(materialized = 'table')}}

WITH sessions AS (

    SELECT 
        session_date,
        session_id,
        user_id,
        has_product_view,
        has_add_to_cart,
        has_purchase_event
    FROM {{ ref('fct_sessions') }}
),

map AS (

    SELECT
        payment_intent_id,
        session_id
    FROM {{ ref('map_payment_to_session')}}
    WHERE session_id IS NOT NULL
),

paid_sessions AS (

    SELECT 
        m. session_id
    FROM map m
    JOIN {{ref('fct_payments')}} p 
        ON m.payment_intent_id = p.payment_intent_id
    WHERE p.is_succeeded IS TRUE 
),

session_rollup AS (

    SELECT 
        s.session_date,

        count(*) AS sessions,

        sum(CASE WHEN s.has_product_view THEN 1 ELSE 0 END) AS sessions_with_view,
        sum(CASE WHEN s.has_add_to_cart THEN 1 ELSE 0 END) AS sessions_with_cart,
        sum(CASE WHEN s.has_purchase_event THEN 1 ELSE 0 END) AS sessions_with_purchase_event,
        sum(CASE WHEN ps.session_id IS NOT NULL THEN 1 ELSE 0 END) AS sessions_with_payment
    FROM sessions s 
    LEFT JOIN paid_sessions ps 
        ON ps.session_id = s.session_id
    GROUP BY 1
),

final AS (
    SELECT 
        session_date,

        sessions,
        sessions_with_view,
        sessions_with_cart,
        sessions_with_purchase_event,
        sessions_with_payment,

        --conversions 
        sessions_with_view * 1.0/ NULLIF(sessions, 0) AS session_to_view_cr,
        sessions_with_cart * 1.0 / nullif(sessions_with_view, 0) as view_to_cart_cr,
        sessions_with_purchase_event * 1.0 / nullif(sessions_with_cart,0) as cart_to_purchase_event_cr,
        sessions_with_payment * 1.0 / nullif(sessions_with_purchase_event,0) as purchase_event_to_payment_cr,
        sessions_with_payment * 1.0 / nullif(sessions_with_view, 0) as view_to_payment_cr,
        sessions_with_payment * 1.0 / nullif(sessions, 0) as session_to_payment_cr

    FROM session_rollup
)

SELECT * FROM final