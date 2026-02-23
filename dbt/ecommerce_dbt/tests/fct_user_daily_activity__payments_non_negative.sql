select *
from {{ ref('fct_user_daily_activity') }}
where payments_count < 0