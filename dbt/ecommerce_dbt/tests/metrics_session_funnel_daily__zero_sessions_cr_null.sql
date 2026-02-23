select *
from {{ ref('metrics_session_funnel_daily') }}
where sessions = 0
  and (
    session_to_view_cr is not null
    or session_to_payment_cr is not null
  )