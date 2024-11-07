{{config(materialized='table')}}

WITH fact_table AS (
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
    FROM {{ ref('mart_stageing_yellow_taxi_data') }}
)

SELECT
    yellow_taxi_id,
    BOROUGH,
    ZONE,
    DOLOCATIONID,
    PULOCATIONID,
    RATECODEID,
    VENDORID,
    COUNT(*) AS total_orders,
    SUM(CONGESTION_SURCHARGE) AS total_congestion_surcharge,
    SUM(EXTRA) AS total_extra,
    SUM(FARE_AMOUNT) AS total_fare_amount,
    SUM(IMPROVEMENT_SURCHARGE) AS total_improvement_surcharge,
    SUM(MTA_TAX) AS total_mta_tax,
    COUNT(PASSENGER_COUNT) AS total_passenger_count,  -- Count of non-null passenger counts
    CASE
        WHEN PAYMENT_TYPE = 1 THEN 'creditcard'
        WHEN PAYMENT_TYPE = 2 THEN 'cash'
        WHEN PAYMENT_TYPE = 3 THEN 'no charge'
        WHEN PAYMENT_TYPE = 4 THEN 'dispute'
        ELSE 'unknown'
    END AS payment_type,
    COUNT(STORE_AND_FWD_FLAG) AS total_store_and_fwd_flag,  -- Count of non-null flags
    SUM(TIP_AMOUNT) AS total_tip_amount,
    SUM(TOLLS_AMOUNT) AS total_tolls_amount,
    SUM(TOTAL_AMOUNT) AS total_total_amount,
    MAX(TPEP_DROPOFF_DATETIME) AS latest_dropoff_datetime,  -- Use MAX to avoid grouping issues with datetime
    MAX(TPEP_PICKUP_DATETIME) AS latest_pickup_datetime,  -- Use MAX to avoid grouping issues with datetime
    SUM(TRIP_DISTANCE) AS total_trip_distance
FROM fact_table
GROUP BY
    yellow_taxi_id,
    BOROUGH,
    ZONE,
    DOLOCATIONID,
    PULOCATIONID,
    RATECODEID,
    VENDORID,
    payment_type
