


-- tests/test_latest_dropoff_datetime.sql
SELECT

 latest_dropoff_datetime
 
FROM {{ ref('greentaxi_fact_table') }}
WHERE latest_dropoff_datetime IS NULL
