select *
from {{ ref('metrics_unit_economics_daily') }}
where active_users < 0