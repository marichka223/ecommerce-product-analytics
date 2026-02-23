select *
from {{ ref('dim_products') }}
where first_seen_at > last_seen_at