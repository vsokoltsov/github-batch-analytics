-- Warehouse note: source marts are partitioned by dt and hr in Athena.
-- Repositories are bucketed by repo_id and organizations are bucketed by org_id before dashboard queries run.

select
    repo_id,
    repo_full_name,
    owner_login,
    language,
    total_events,
    unique_actors,
    composite_score,
    stargazers_count,
    forks_count
from repo_marts
order by composite_score desc, total_events desc
limit 100
