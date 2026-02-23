select *
from {{ ref('fct_sessions') }}
where session_duration_seconds < 0