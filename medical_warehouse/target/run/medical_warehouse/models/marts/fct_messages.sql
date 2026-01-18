
  
    

  create  table "neondb"."public"."fct_messages__dbt_tmp"
  
  
    as
  
  (
    select
    s.message_id,
    md5(s.channel_name) as channel_key,
    to_char(s.message_date, 'YYYYMMDD')::int as date_key,
    s.message_text,
    s.message_length,
    s.views,
    s.forwards,
    s.has_media
from "neondb"."public"."stg_telegram_messages" s
  );
  