select *
from {{ ref('metrics_dau_wau_mau') }}
where mau < wau