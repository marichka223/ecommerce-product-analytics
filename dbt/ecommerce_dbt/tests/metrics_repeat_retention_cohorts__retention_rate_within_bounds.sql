select *
from {{ ref('metrics_repeat_retention_cohorts') }}
where retention_rate < 0 or retention_rate > 1