{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'payment_intent_id',
    on_schema_change = 'sync_all_columns'
)}}

{% set lookback_days = var('lookback_days', 3) %}
WITH pi AS(

    SELECT
        payment_intent_id,
        event_time,
        received_ts,

        status,
        amount_gross AS payment_intent_amount,
        currency AS payment_intent_currency,

        latest_charge_id,
        order_id,
        attempt_number,
        payment_method,

        user_id,
        user_session

    FROM {{ref('stripe_payment_intents')}}
    
    {% if is_incremental()%}
    WHERE received_ts > (SELECT coalesce(max(last_received_ts), TIMESTAMP '1900-01-01')
        FROM {{this}}) - INTERVAL '{{lookback_days}} days'
    {% endif %}
),

charges_ranked AS(
    SELECT
        c.*,
        row_number() OVER(
            PARTITION BY c.payment_intent_id
            ORDER BY 
                CASE WHEN lower(c.status) = 'succeeded' THEN 1 ELSE 0 END DESC,
                c.event_time DESC,
                c.received_ts DESC
        ) AS rn_best,

        min(CASE WHEN lower(c.status) = 'succeeded' THEN c.event_time END)
        OVER(PARTITION BY c.payment_intent_id) AS first_succeeded_at
    FROM {{ ref('stripe_charges')}} c 
    WHERE c.payment_intent_id IS NOT NULL
),

charge_best AS(
    SELECT
        payment_intent_id,
        charge_id,
        status AS charge_status,
        amount_gross AS charge_amount,
        currency AS charge_currency,
        fee,
        amount_net,
        event_time AS charge_event_time,
        first_succeeded_at AS succeeded_at
    FROM charges_ranked
    WHERE rn_best = 1
),

refunds_by_pi AS (
    SELECT
        c.payment_intent_id,
        sum(CASE WHEN lower(r.status) = 'succeeded' or r.status IS NULL THEN r.amount_refunded ELSE 0 END) AS refunded_amount
    FROM {{ ref('stripe_refunds')}} r 
    LEFT JOIN {{ ref('stripe_charges')}} c ON r.charge_id = c.charge_id 
    GROUP BY 1
),

final AS(

    SELECT
        pi.payment_intent_id,
        pi.event_time,

        cb.succeeded_at,
        CASE 
            WHEN cb.succeeded_at IS NOT NULL OR lower(pi.status) = 'succeeded' THEN TRUE 
            ELSE FALSE 
            END AS is_succeeded,
        pi.status,

        -- amount/currency
        coalesce(pi.payment_intent_amount, cb.charge_amount) AS amount,
        coalesce(pi.payment_intent_currency, cb.charge_currency) AS currency,

        cb.charge_id,

        -- refunds
        coalesce(rp.refunded_amount, 0) AS refunded_amount,
        CASE WHEN coalesce(rp.refunded_amount, 0) >0 THEN TRUE ELSE FALSE END AS is_refunded,

        -- metadata
        pi.user_id,
        pi.user_session,
        pi.order_id,

        pi.attempt_number,
        pi.payment_method,

        pi.received_ts AS last_received_ts
    FROM pi
    LEFT JOIN charge_best cb 
        ON cb.payment_intent_id = pi.payment_intent_id
    LEFT JOIN refunds_by_pi rp
    ON rp.payment_intent_id = pi.payment_intent_id
)

SELECT 
    payment_intent_id,
    event_time AS payment_intent_event_ts,
    
    succeeded_at,
    is_succeeded,
    status,
    
    amount,
    currency,
    charge_id,
    
    refunded_amount,
    is_refunded,
    
    user_id,
    user_session,
    order_id,
    
    attempt_number,
    payment_method,
    
    last_received_ts
 FROM final