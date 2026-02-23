select *
from {{ ref('metrics_product_conversion_daily') }}
where payments_count = 0
  and avg_order_value is not null