select *
from {{ ref('fct_user_daily_activity') }}
where revenue_amount < 0