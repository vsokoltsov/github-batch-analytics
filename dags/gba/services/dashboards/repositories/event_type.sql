select
    event_type,
    event_count
from (
    select 'push_events' as event_type, sum(push_events) as event_count from repo_marts
    union all
    select 'pull_request_events', sum(pull_request_events) from repo_marts
    union all
    select 'issue_comment_events', sum(issue_comment_events) from repo_marts
    union all
    select 'issues_events', sum(issues_events) from repo_marts
    union all
    select 'fork_events', sum(fork_events) from repo_marts
    union all
    select 'watch_events', sum(watch_events) from repo_marts
)
