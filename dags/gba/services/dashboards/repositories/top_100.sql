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
