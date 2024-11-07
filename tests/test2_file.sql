  select 
  TOTAL_TRIP_DISTANCE 
  
FROM {{ ref('greentaxi_fact_table') }}
where TOTAL_TRIP_DISTANCE <=0