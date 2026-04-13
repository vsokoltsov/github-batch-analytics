select
    coalesce(r.language, 'Unknown') as language,
    coalesce(o.location, 'Unknown') as org_location,
    count(*) as repo_count,
    sum(r.total_events) as total_events
from repo_marts r
left join org_marts o
on r.owner_login = o.org_login
group by
    coalesce(r.language, 'Unknown'),
    coalesce(o.location, 'Unknown')