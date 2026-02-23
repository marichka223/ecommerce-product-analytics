select *
from {{ ref('fct_sessions') }}
where session_start_at > session_end_at