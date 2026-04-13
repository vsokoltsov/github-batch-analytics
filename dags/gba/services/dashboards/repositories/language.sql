select
    coalesce(language, 'Unknown') as "language", -- noqa: RF04
    count(*) as repo_count,
    sum(total_events) as total_events,
    avg(composite_score) as avg_composite_score,
    sum(stargazers_count) as total_stargazers
from repo_marts
group by coalesce(language, 'Unknown')
