select *
from {{ ref('dim_users') }}
where lifetime_revenue < 0