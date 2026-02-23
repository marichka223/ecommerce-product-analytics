select *
from {{ ref('fct_product_daily_activity') }}
where payments_count < 0