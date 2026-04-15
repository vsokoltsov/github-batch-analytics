-- Warehouse note: source marts are partitioned by dt and hr in Athena.
-- Repositories are bucketed by repo_id and organizations are bucketed by org_id before dashboard queries run.

select
    coalesce(location, 'Unknown') as location, -- noqa: RF04
    count(*) as org_count,
    sum(total_events) as total_events,
    avg(followers) as avg_followers
from org_marts
group by coalesce(location, 'Unknown')
