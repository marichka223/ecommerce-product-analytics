select *
from {{ ref('fct_payments') }}
where refunded_amount > amount