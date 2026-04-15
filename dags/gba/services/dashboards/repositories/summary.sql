-- Warehouse note: source marts are partitioned by dt and hr in Athena.
-- Repositories are bucketed by repo_id and organizations are bucketed by org_id before dashboard queries run.

select
    repo_id,
    repo_full_name,
    max(repo_name) as repo_name,
    max(owner_login) as owner_login,
    max(owner_type) as owner_type,
    max(language) as language, -- noqa: RF04
    max(visibility) as visibility,
    max(is_fork) as is_fork,
    max(is_archived) as is_archived,
    max(is_disabled) as is_disabled,
    max(stargazers_count) as stargazers_count,
    max(forks_count) as forks_count,
    max(watchers_count) as watchers_count,
    max(subscribers_count) as subscribers_count,
    max(open_issues_count) as open_issues_count,
    sum(total_events) as total_events,
    sum(push_events) as push_events,
    sum(pull_request_events) as pull_request_events,
    sum(issue_comment_events) as issue_comment_events,
    sum(fork_events) as fork_events,
    avg(composite_score) as avg_composite_score,
    avg(bot_ratio) as avg_bot_ratio
from repo_marts
group by repo_id, repo_full_name
