select 
*
from
    {{source('snowflake_database','dbt_lookup_table')}}