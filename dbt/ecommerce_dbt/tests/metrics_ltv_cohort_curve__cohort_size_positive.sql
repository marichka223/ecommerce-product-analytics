select *
from {{ ref('metrics_ltv_cohort_curve') }}
where cohort_size <= 0