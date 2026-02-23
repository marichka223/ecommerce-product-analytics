select *
from {{ ref('dim_users') }}
where is_payer = true
  and first_purchase_at is null