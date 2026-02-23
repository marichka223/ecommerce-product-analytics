select *
from {{ ref('metrics_product_conversion_daily') }}
where views_count < 0
   or add_to_cart_count < 0
   or payments_count < 0