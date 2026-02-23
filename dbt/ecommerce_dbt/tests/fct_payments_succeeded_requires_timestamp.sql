select *
from {{ ref('fct_payments') }}
where is_succeeded = true
  and succeeded_at is null