select *
from {{ ref('fct_payments') }}
where is_refunded = true
  and refunded_amount <= 0