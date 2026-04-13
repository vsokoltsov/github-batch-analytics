select
    case
        when datediff(current_timestamp(), pushed_at) <= 7 then '0-7d'
        when datediff(current_timestamp(), pushed_at) <= 30 then '8-30d'
        when datediff(current_timestamp(), pushed_at) <= 90 then '31-90d'
        else '90d+'
    end as freshness_bucket,
    count(*) as repo_count,
    sum(total_events) as total_events
from repo_marts
group by
    case
        when datediff(current_timestamp(), pushed_at) <= 7 then '0-7d'
        when datediff(current_timestamp(), pushed_at) <= 30 then '8-30d'
        when datediff(current_timestamp(), pushed_at) <= 90 then '31-90d'
        else '90d+'
    end