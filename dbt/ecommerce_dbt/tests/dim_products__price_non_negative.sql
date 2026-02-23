select *
from {{ ref('dim_products') }}
where price < 0