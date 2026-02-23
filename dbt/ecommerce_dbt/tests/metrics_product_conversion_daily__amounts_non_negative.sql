select *
from {{ ref('metrics_product_conversion_daily') }}
where revenue_amount < 0
   or refunded_amount < 0