select *
from {{ ref('fct_user_daily_activity') }}
where sessions_count < 0