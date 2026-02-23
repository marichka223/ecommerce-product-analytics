select *
from {{ ref('metrics_retention_cohorts') }}
where retained_users > cohort_sizes