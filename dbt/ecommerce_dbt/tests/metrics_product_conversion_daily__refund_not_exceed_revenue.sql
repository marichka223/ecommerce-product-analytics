select *
from {{ ref('metrics_product_conversion_daily') }}
where refunded_amount > revenue_amount