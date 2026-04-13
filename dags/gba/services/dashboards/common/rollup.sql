select
    r.repo_id,
    r.repo_full_name,
    max(r.repo_name) as repo_name,
    max(r.owner_login) as owner_login,
    max(r.language) as language, -- noqa: RF04
    sum(r.total_events) as repo_total_events,
    avg(r.composite_score) as repo_avg_composite_score,
    max(r.stargazers_count) as stargazers_count,
    max(o.org_id) as org_id,
    max(o.org_name) as org_name,
    max(o.location) as org_location,
    max(o.company) as org_company,
    max(o.followers) as org_followers,
    max(o.public_repos) as org_public_repos,
    max(o.is_verified) as org_is_verified
from repo_marts as r
left join org_marts as o
    on r.owner_login = o.org_login
group by r.repo_id, r.repo_full_name;
