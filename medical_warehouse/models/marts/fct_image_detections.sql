/*
  Enriched fact table: links YOLO image categories to messages.
  Enables analysis like: "Do promotional posts get more views?"
*/
select
    y.message_id::bigint,
    md5(y.channel_name) as channel_key,
    to_char(s.message_date, 'YYYYMMDD')::int as date_key,
    y.detected_objects,
    y.confidence_score,
    y.image_category
from {{ source('raw', 'yolo_detections') }} y
join {{ ref('stg_telegram_messages') }} s
    on y.message_id::text = s.message_id::text
    and y.channel_name = s.channel_name