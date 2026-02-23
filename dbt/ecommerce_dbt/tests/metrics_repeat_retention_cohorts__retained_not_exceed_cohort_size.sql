select *
from {{ ref('metrics_repeat_retention_cohorts') }}
where retained_users > cohort_size