select *
from {{ ref('metrics_retention_cohorts') }}
where cohort_date > current_date