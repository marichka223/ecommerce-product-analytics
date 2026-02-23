select *
from {{ ref('fct_events') }}
where event_type not in (
    'view',
    'cart',
    'purchase'
)