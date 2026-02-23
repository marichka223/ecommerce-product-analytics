select *
from {{ ref('dim_users') }}
where first_purchase_at is null
  and lifetime_revenue <> 0