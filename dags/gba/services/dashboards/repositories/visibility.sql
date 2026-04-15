-- Warehouse note: source marts are partitioned by dt and hr in Athena.
-- Repositories are bucketed by repo_id and organizations are bucketed by org_id before dashboard queries run.

select
    coalesce(visibility, 'Unknown') as visibility,
    count(*) as repo_count,
    sum(total_events) as total_events
from repo_marts
group by coalesce(visibility, 'Unknown');
