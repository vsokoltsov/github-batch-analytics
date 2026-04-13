select
    case when is_verified then 'verified' else 'not_verified' end
        as verification_status,
    count(*) as org_count,
    sum(total_events) as total_events,
    avg(composite_score) as avg_composite_score
from org_marts
group by case when is_verified then 'verified' else 'not_verified' end
