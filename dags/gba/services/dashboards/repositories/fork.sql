select
    case when is_fork then 'fork' else 'original' end as fork_status,
    count(*) as repo_count,
    sum(total_events) as total_events,
    avg(composite_score) as avg_composite_score
from repo_marts
group by case when is_fork then 'fork' else 'original' end
