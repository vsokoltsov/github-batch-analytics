select
    coalesce(company, 'Unknown') as company,
    count(*) as org_count,
    sum(total_events) as total_events
from org_marts
group by coalesce(company, 'Unknown')
