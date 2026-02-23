select *
from {{ ref('metrics_ltv_cohort_curve') }}
where cumulative_revenue < 0