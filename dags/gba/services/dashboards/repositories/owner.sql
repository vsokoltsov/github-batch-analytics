select
    coalesce(owner_type, 'Unknown') as owner_type,
    count(*) as repo_count,
    sum(total_events) as total_events,
    avg(composite_score) as avg_composite_score
from repo_marts
group by coalesce(owner_type, 'Unknown')
