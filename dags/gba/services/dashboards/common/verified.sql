select
    case when o.is_verified then 'verified' else 'not_verified' end
        as verification_status,
    count(*) as repo_count,
    sum(r.total_events) as total_events,
    avg(r.composite_score) as avg_repo_score
from repo_marts as r
left join org_marts as o
    on r.owner_login = o.org_login
group by case when o.is_verified then 'verified' else 'not_verified' end
