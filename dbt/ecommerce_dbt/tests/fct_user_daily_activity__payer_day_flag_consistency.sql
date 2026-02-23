select *
from {{ ref('fct_user_daily_activity') }}
where (payments_count > 0 and is_payer_day = false)
   or (payments_count = 0 and is_payer_day = true)