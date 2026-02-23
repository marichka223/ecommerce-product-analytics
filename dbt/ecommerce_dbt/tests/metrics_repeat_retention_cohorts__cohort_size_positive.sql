select *
from {{ ref('metrics_repeat_retention_cohorts') }}
where cohort_size <= 0