select *
from {{ ref('fct_product_daily_activity') }}
where refunded_amount > revenue_amount