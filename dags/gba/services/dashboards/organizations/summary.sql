select
    org_id,
    org_login,
    max(org_name) as org_name,
    max(location) as location,
    max(company) as company,
    max(blog) as blog,
    max(email) as email,
    max(twitter_username) as twitter_username,
    max(is_verified) as is_verified,
    max(has_organization_projects) as has_organization_projects,
    max(has_repository_projects) as has_repository_projects,
    max(public_repos) as public_repos,
    max(public_gists) as public_gists,
    max(followers) as followers,
    max(following) as following,
    sum(total_events) as total_events,
    sum(push_events) as push_events,
    sum(pull_request_events) as pull_request_events,
    avg(composite_score) as avg_composite_score,
    avg(bot_ratio) as avg_bot_ratio
from org_marts
group by org_id, org_login;