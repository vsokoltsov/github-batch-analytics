-- Warehouse note: source marts are partitioned by dt and hr in Athena.
-- Repositories are bucketed by repo_id and organizations are bucketed by org_id before dashboard queries run.

select
    case when is_fork then 'fork' else 'original' end as fork_status,
    count(*) as repo_count,
    sum(total_events) as total_events,
    avg(composite_score) as avg_composite_score
from repo_marts
group by case when is_fork then 'fork' else 'original' end
