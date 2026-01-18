/*
  CUSTOM TEST: No messages should have future dates
  This enforces a business rule: data must be from the past/present
  PASS = returns 0 rows
*/

select *
from "neondb"."public"."stg_telegram_messages"
where message_date > current_timestamp