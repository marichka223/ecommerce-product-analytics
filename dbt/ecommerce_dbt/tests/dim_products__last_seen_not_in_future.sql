select *
from {{ ref('dim_products') }}
where last_seen_at > current_timestamp