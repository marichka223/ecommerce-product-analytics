select *
from {{ ref('dim_products') }}
where events_count <= 0