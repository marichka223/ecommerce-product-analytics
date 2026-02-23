select *
from {{ ref('fct_user_daily_activity') }}
where (sessions_count > 0 and is_active = false)
   or (sessions_count = 0 and is_active = true)