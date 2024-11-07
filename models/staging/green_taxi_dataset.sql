select
*
from 
    {{source('snowflake_database','dbt_green_taxi_data')}}