{{ config(materialized='table') }}

WITH base AS (

    SELECT
        activity_date,
        product_id,

        coalesce(views_count, 0) AS views_count,
        coalesce(add_to_cart_count, 0) AS add_to_cart_count,
        coalesce(payments_count, 0) AS payments_count,

        coalesce(revenue_amount, 0) AS revenue_amount,
        coalesce(refunded_amount, 0) AS refunded_amount
    FROM {{ ref('fct_product_daily_activity') }}

),

final AS (

    SELECT
        activity_date,
        product_id,

        views_count,
        add_to_cart_count,
        payments_count,

        revenue_amount,
        refunded_amount,

        -- conversions
        add_to_cart_count * 1.0 / nullif(views_count, 0) AS view_to_cart_cr,
        payments_count * 1.0 / nullif(add_to_cart_count, 0) AS cart_to_payment_cr,
        payments_count * 1.0 / nullif(views_count, 0) AS view_to_payment_cr,

        -- unit economics per product-day
        revenue_amount * 1.0 / nullif(payments_count, 0) AS avg_order_value,
        refunded_amount * 1.0 / nullif(revenue_amount, 0) AS refund_rate

    FROM base

)

SELECT *
FROM final