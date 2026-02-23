select *
from {{ ref('metrics_dau_wau_mau') }}
where dau < 0