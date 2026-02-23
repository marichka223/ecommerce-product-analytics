select *
from {{ ref('metrics_retention_cohorts') }}
where cohort_sizes <= 0