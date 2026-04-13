select
    org_id,
    org_login,
    org_name,
    location,
    public_repos,
    followers,
    repos_count,
    total_events,
    composite_score
from org_marts
order by composite_score desc, total_events desc
limit 100
