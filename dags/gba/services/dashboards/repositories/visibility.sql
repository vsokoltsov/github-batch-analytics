select
    coalesce(visibility, 'Unknown') as visibility,
    count(*) as repo_count,
    sum(total_events) as total_events
from repo_marts
group by coalesce(visibility, 'Unknown');
