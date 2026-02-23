select *
from {{ ref('fct_product_daily_activity') }}
where (payments_count > 0 and had_payment = false)
   or (payments_count = 0 and had_payment = true)