-- Warehouse note: source marts are partitioned by dt and hr in Athena.
-- Repositories are bucketed by repo_id and organizations are bucketed by org_id before dashboard queries run.

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
