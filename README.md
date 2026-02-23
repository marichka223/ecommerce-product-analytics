# Business Context

This project simulates the analytics environment of an e-commerce platform in the consumer electronics category.

Over a 60-day observed window, the business wanted to understand:

- Why revenue fluctuates around promotional periods
- Where users drop off in the purchase funnel
- Whether growth is driven by new or repeat buyers
- How quickly cohorts generate value (LTV)
- What drives retention and repeat purchase behavior

The objective was not just to compute metrics, but to build a structured analytical layer enabling product decision-making.

---

# Key Business Questions

1. Where do we lose users in the funnel?
2. Is revenue growth driven by traffic or improved conversion?
3. Are we acquiring high-quality users who return?
4. How much value does a new cohort generate over time?
5. How dependent are we on repeat buyers?
6. Which brands drive sustainable retention?

---

# Example Insights

- Mid-October revenue increase aligns with smartphone product releases.
- A conversion spike in early November reflects pre-Black Friday consideration behavior.
- Repeat buyers represent a small share of users but contribute disproportionately to revenue.
- Largest funnel drop occurs between Product View and Add to Cart.

Full business narrative and insights are presented in:

`/PRESENTATION.pdf`

---

# Architecture

The project follows a standard dbt layering approach:

`staging → intermediate → marts/core → marts/metrics`

## Staging

Raw source normalization:

- `stg_clickstream_events`
- `stg_stripe_events`

## Intermediate

Cleaning, deduplication and matching logic:

- `clickstream_events_clean`
- `stripe_payment_intents`
- `stripe_charges`
- `stripe_refunds`
- `bridge_payment_event_candidates`
- `map_payment_to_event`
- `map_payment_to_session`

## Core Layer (Canonical Facts)

Stable business entities with defined grain:

- `fct_events` — 1 row = 1 event
- `fct_sessions` — 1 row = 1 session
- `fct_payments` — 1 row = 1 payment_intent
- `fct_user_daily_activity` — 1 row = 1 user per day
- `fct_product_daily_activity` — 1 row = 1 product per day
- `dim_users`
- `dim_products`

## Metrics Layer

Business KPI models:

- `metrics_session_funnel_daily`
- `metrics_product_conversion_daily`
- `metrics_retention_cohorts`
- `metrics_ltv_cohort_curve`
- `metrics_unit_economics_daily`
- `metrics_revenue_retention_cohorts`

---

# Key KPIs Implemented

## Engagement

- DAU / WAU / MAU
- Sessions per user
- Bounce rate

## Funnel

- Session → Payment CR
- View → Cart CR
- Cart → Payment CR
- Payment success rate
- Time to purchase

## Monetization

- Revenue
- Revenue per session
- ARPU
- ARPPU
- Payer rate
- Revenue by brand

## Retention

- D1 / D7 / D30 retention
- Cohort heatmap
- Revenue retention

## LTV

- Cohort LTV curve
- D7 / D30 / D60 LTV snapshots

---

# Tech Stack

- dbt
- SQL
- (DuckDB / BigQuery / Snowflake — specify yours)
- BI tool: (Metabase / Looker Studio / Tableau Public)

---

# Data Quality & Modeling Practices

- Explicit grain definitions
- Incremental models with lookback windows
- Matching logic for payment → session resolution

Schema tests:

- `not_null`
- `unique`
- `relationships`

- Separation between canonical facts and business metrics

---

# What This Project Demonstrates

This project demonstrates:

- Ability to design a layered analytics warehouse
- Product thinking beyond raw SQL
- Understanding of engagement, monetization and retention
- Cohort and LTV modeling
- Conversion analysis and funnel diagnostics
- Analytical storytelling via dashboards