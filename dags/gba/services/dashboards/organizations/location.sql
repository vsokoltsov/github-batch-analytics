select
    coalesce(location, 'Unknown') as "location", -- noqa: RF04
    count(*) as org_count,
    sum(total_events) as total_events,
    avg(followers) as avg_followers
from org_marts
group by coalesce(location, 'Unknown')
