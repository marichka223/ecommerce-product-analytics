select *
from {{ ref('fct_user_daily_activity') }}
where refunded_amount > revenue_amount