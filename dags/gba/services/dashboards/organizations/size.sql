select
    case
        when repos_count <= 5 then '1-5'
        when repos_count <= 20 then '6-20'
        when repos_count <= 100 then '21-100'
        else '100+'
    end as size_bucket,
    count(*) as org_count,
    sum(total_events) as total_events,
    avg(followers) as avg_followers
from org_marts
group by
    case
        when repos_count <= 5 then '1-5'
        when repos_count <= 20 then '6-20'
        when repos_count <= 100 then '21-100'
        else '100+'
    end
