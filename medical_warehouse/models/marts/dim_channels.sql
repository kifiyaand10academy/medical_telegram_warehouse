select
    md5(channel_name) as channel_key,
    channel_name,
    case
        when channel_name like '%pharma%' then 'Pharmaceutical'
        when channel_name like '%cosmetic%' then 'Cosmetics'
        else 'Medical'
    end as channel_type,
    min(message_date) as first_post_date,
    max(message_date) as last_post_date,
    count(*) as total_posts,
    avg(views) as avg_views
from {{ ref('stg_telegram_messages') }}
group by 1, 2, 3