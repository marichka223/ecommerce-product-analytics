select *
from {{ ref('fct_events') }}
where price < 0