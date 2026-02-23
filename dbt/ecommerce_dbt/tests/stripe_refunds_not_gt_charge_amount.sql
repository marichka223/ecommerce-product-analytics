select r.*
from {{ ref('stripe_refunds') }} r
join {{ ref('stripe_charges') }} c
  on r.charge_id = c.charge_id
where r.amount_refunded > c.amount_gross