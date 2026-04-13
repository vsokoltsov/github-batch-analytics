select
    case when twitter_username is not null then 'has_twitter' else 'no_twitter' end as
twitter_status,
    case when blog is not null then 'has_blog' else 'no_blog' end as blog_status,
    case when email is not null then 'has_email' else 'no_email' end as email_status,
    count(*) as org_count
from org_marts
group by
    case when twitter_username is not null then 'has_twitter' else 'no_twitter' end,
    case when blog is not null then 'has_blog' else 'no_blog' end,
    case when email is not null then 'has_email' else 'no_email' end