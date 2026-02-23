select *
from {{ ref('metrics_retention_cohorts') }}
where cohort_age_days < 0