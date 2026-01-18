/*
  STAGING MODEL: stg_telegram_messages

  Purpose:
    - Take raw data from `raw.telegram_messages`
    - Clean it, standardize it, and prepare for modeling
    - This is your "clean room" before building analytics tables

  Why?
    - Raw data may have bad types, missing values, or inconsistent formats
    - We fix that here so marts are trustworthy
*/

select
    -- Primary key
    message_id::bigint as message_id,

    -- Standardize channel name (lowercase for consistency)
    lower(channel_name)::text as channel_name,

    -- Fix date: ensure it's a proper timestamp
    message_date::timestamp as message_date,

    -- Handle missing text: replace NULL with empty string
    coalesce(message_text, '')::text as message_text,

    -- Numeric fields: treat NULL as 0
    coalesce(views, 0)::int as views,
    coalesce(forwards, 0)::int as forwards,

    -- Boolean flag: did this message have an image?
    coalesce(has_media, false)::bool as has_media,

    -- Add useful derived field
    length(coalesce(message_text, ''))::int as message_length

from "neondb"."raw"."telegram_messages"

-- Filter out garbage
where 
    message_id is not null
    and message_date is not null
    and message_date <= current_timestamp  -- no future dates!