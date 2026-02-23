select *
from {{ ref('fct_payments') }}
where is_succeeded = true
  and amount <= 0