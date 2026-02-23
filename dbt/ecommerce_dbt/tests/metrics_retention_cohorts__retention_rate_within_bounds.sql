select *
from {{ ref('metrics_retention_cohorts') }}
where retention_rate < 0 or retention_rate > 1