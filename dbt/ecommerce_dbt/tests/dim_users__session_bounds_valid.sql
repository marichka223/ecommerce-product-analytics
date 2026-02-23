select *
from {{ ref('dim_users') }}
where first_session_at > last_session_at