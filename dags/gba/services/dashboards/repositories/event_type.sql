select
    event_type,
    event_count
from (
    select
        'push_events' as event_type,
        sum(push_events) as event_count
    from repo_marts
    union all
    select
        'pull_request_events' as event_type, -- noqa: AL03
        sum(pull_request_events) as event_count -- noqa: AL03
    from repo_marts
    union all
    select
        'issue_comment_events' as event_type, -- noqa: AL03
        sum(issue_comment_events) as event_count -- noqa: AL03
    from repo_marts
    union all
    select
        'issues_events' as event_type, -- noqa: AL03
        sum(issues_events) as event_count -- noqa: AL03
    from repo_marts
    union all
    select
        'fork_events' as event_type, -- noqa: AL03
        sum(fork_events) as event_count -- noqa: AL03
    from repo_marts
    union all
    select
        'watch_events' as event_type, -- noqa: AL03
        sum(watch_events) as event_count -- noqa: AL03
    from repo_marts
)
