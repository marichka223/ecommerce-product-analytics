select *
from {{ ref('fct_events') }}
where event_time > current_timestamp