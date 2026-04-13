select
    coalesce(r.language, 'Unknown') as language, -- noqa: RF04
    coalesce(o.location, 'Unknown') as org_location,
    count(*) as repo_count,
    sum(r.total_events) as total_events
from repo_marts as r
left join org_marts as o
    on r.owner_login = o.org_login
group by
    coalesce(r.language, 'Unknown'),
    coalesce(o.location, 'Unknown')
