{{config(materialized='table')}}

WITH yellow_taxi_data AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['DBT_YELLOW_ID','VENDORID']) }} AS yellow_taxi_id,
        
        DOLOCATIONID,
        PULOCATIONID,
        RATECODEID,
        VENDORID,
        CONGESTION_SURCHARGE,
        EXTRA,
        FARE_AMOUNT,
        IMPROVEMENT_SURCHARGE,
        MTA_TAX,
        PASSENGER_COUNT,
        PAYMENT_TYPE,
        STORE_AND_FWD_FLAG,
        TIP_AMOUNT,
        TOLLS_AMOUNT,
        TOTAL_AMOUNT,
        TPEP_DROPOFF_DATETIME,
        TPEP_PICKUP_DATETIME,
        TRIP_DISTANCE
    from {{ref('yellow_taxi_dataset')}}
),
lookup_data AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['LOCATIONID']) }} AS dbt_lookup_id,  -- updated macro
        LOCATIONID,
        BOROUGH,
        ZONE,
        SERVICE_ZONE
    FROM {{ ref('lookup_table') }}
)
SELECT 
    yellow_taxi_id,
    DOLOCATIONID,
    PULOCATIONID,
    RATECODEID,
    VENDORID,
    CONGESTION_SURCHARGE,
    EXTRA,
    FARE_AMOUNT,
    IMPROVEMENT_SURCHARGE,
    MTA_TAX,
    PASSENGER_COUNT,
    PAYMENT_TYPE,
    STORE_AND_FWD_FLAG,
    TIP_AMOUNT,
    TOLLS_AMOUNT,
    TOTAL_AMOUNT,
    TPEP_DROPOFF_DATETIME,
    TPEP_PICKUP_DATETIME,
    TRIP_DISTANCE,
    BOROUGH,
    ZONE,
    SERVICE_ZONE
FROM yellow_taxi_data
JOIN lookup_data
    ON yellow_taxi_data.DOLOCATIONID = lookup_data.LOCATIONID
    AND yellow_taxi_data.PULOCATIONID = lookup_data.LOCATIONID
