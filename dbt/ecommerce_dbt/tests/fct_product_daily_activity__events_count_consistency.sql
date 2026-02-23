select *
from {{ ref('fct_product_daily_activity') }}
where events_count < (views_count + add_to_cart_count + purchase_events_count)