select *
from {{ ref('metrics_session_funnel_daily') }}
where sessions_with_view > sessions