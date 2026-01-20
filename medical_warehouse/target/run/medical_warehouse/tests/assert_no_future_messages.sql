
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  /*
  CUSTOM TEST: No messages should have future dates
  This enforces a business rule: data must be from the past/present
  PASS = returns 0 rows
*/

select *
from "neondb"."public"."stg_telegram_messages"
where message_date > current_timestamp
  
  
      
    ) dbt_internal_test