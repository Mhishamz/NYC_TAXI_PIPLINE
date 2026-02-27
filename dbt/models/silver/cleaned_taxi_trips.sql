{{
  config(
    materialized='table',
    schema='silver',
    alias='cleaned_taxi_trips',
    tags=['silver', 'cleaned'],
    indexes=[
      {'columns': ['pickup_datetime'], 'type': 'btree'},
      {'columns': ['vendor_id'],       'type': 'btree'},
      {'columns': ['payment_type_code'], 'type': 'btree'},
    ],
    post_hook=[
      "ANALYZE {{ this }}"
    ]
  )
}}

/*
  Silver Layer: cleaned_taxi_trips
  ─────────────────────────────────
  Transforms raw bronze strings into typed, cleaned, validated records.
  Business rules applied:
    - Cast all numeric strings to NUMERIC/INTEGER
    - Parse datetimes from ISO strings
    - Derive trip duration, speed, per-mile cost
    - Map payment_type codes to labels
    - Filter obvious bad records (negative fares, zero distance, etc.)
    - Deduplicate on business key
*/

WITH source AS (

    SELECT *
    FROM {{ source('bronze', 'raw_taxi_trips') }}

),

cast_and_clean AS (

    SELECT
        ingestion_id,
        batch_id,
        ingested_at,

        -- Vendor (safe cast: only cast if looks like a small integer)
        CASE WHEN TRIM(COALESCE(vendor_id, '')) ~ '^[0-9]+(\.0+)?$'
             THEN TRIM(vendor_id)::NUMERIC::INTEGER END            AS vendor_id,

        -- Timestamps
        tpep_pickup_datetime::TIMESTAMP                            AS pickup_datetime,
        tpep_dropoff_datetime::TIMESTAMP                           AS dropoff_datetime,

        -- Passengers & distance
        CASE WHEN TRIM(COALESCE(passenger_count, '')) ~ '^[0-9]+(\.0+)?$'
             THEN TRIM(passenger_count)::NUMERIC::INTEGER END      AS passenger_count,
        CASE WHEN TRIM(COALESCE(trip_distance, '')) ~ '^-?[0-9]+(\.?[0-9]+)?$'
             THEN TRIM(trip_distance)::NUMERIC(10,2) END           AS trip_distance_miles,

        -- Location IDs
        CASE WHEN TRIM(COALESCE(pu_location_id, '')) ~ '^[0-9]+(\.0+)?$'
             THEN TRIM(pu_location_id)::NUMERIC::INTEGER END       AS pickup_location_id,
        CASE WHEN TRIM(COALESCE(do_location_id, '')) ~ '^[0-9]+(\.0+)?$'
             THEN TRIM(do_location_id)::NUMERIC::INTEGER END       AS dropoff_location_id,

        -- Rate & flags
        CASE WHEN TRIM(COALESCE(rate_code_id, '')) ~ '^[0-9]+(\.0+)?$'
             THEN TRIM(rate_code_id)::NUMERIC::INTEGER END         AS rate_code_id,
        TRIM(store_and_fwd_flag)                                   AS store_and_fwd_flag,

        -- Payment
        CASE WHEN TRIM(COALESCE(payment_type, '')) ~ '^[0-9]+(\.0+)?$'
             THEN TRIM(payment_type)::NUMERIC::INTEGER END         AS payment_type_code,
        CASE TRIM(payment_type)
            WHEN '1' THEN 'Credit Card'
            WHEN '2' THEN 'Cash'
            WHEN '3' THEN 'No Charge'
            WHEN '4' THEN 'Dispute'
            WHEN '5' THEN 'Unknown'
            WHEN '6' THEN 'Voided Trip'
            ELSE          'Other'
        END                                                        AS payment_type_label,

        -- Fare components (safe cast: NULL if not a valid decimal)
        CASE WHEN fare_amount ~ '^-?[0-9]+(\.?[0-9]+)?$'
             THEN TRIM(fare_amount)::NUMERIC(10,2) END             AS fare_amount,
        CASE WHEN extra ~ '^-?[0-9]+(\.?[0-9]+)?$'
             THEN TRIM(extra)::NUMERIC(10,2) END                   AS extra,
        CASE WHEN mta_tax ~ '^-?[0-9]+(\.?[0-9]+)?$'
             THEN TRIM(mta_tax)::NUMERIC(10,2) END                 AS mta_tax,
        CASE WHEN tip_amount ~ '^-?[0-9]+(\.?[0-9]+)?$'
             THEN TRIM(tip_amount)::NUMERIC(10,2) END              AS tip_amount,
        CASE WHEN tolls_amount ~ '^-?[0-9]+(\.?[0-9]+)?$'
             THEN TRIM(tolls_amount)::NUMERIC(10,2) END            AS tolls_amount,
        CASE WHEN improvement_surcharge ~ '^-?[0-9]+(\.?[0-9]+)?$'
             THEN TRIM(improvement_surcharge)::NUMERIC(10,2) END   AS improvement_surcharge,
        CASE WHEN congestion_surcharge ~ '^-?[0-9]+(\.?[0-9]+)?$'
             THEN TRIM(congestion_surcharge)::NUMERIC(10,2) END    AS congestion_surcharge,
        CASE WHEN airport_fee ~ '^-?[0-9]+(\.?[0-9]+)?$'
             THEN TRIM(airport_fee)::NUMERIC(10,2) END             AS airport_fee,
        CASE WHEN total_amount ~ '^-?[0-9]+(\.?[0-9]+)?$'
             THEN TRIM(total_amount)::NUMERIC(10,2) END            AS total_amount

    FROM source
    WHERE tpep_pickup_datetime IS NOT NULL
      AND tpep_dropoff_datetime IS NOT NULL
      AND total_amount IS NOT NULL

),

enrich AS (

    SELECT
        *,
        -- Derived metrics
        EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime)) / 60.0
                                                                    AS trip_duration_mins,
        CASE
            WHEN trip_distance_miles > 0
             AND EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime)) > 0
            THEN trip_distance_miles
                 / (EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime)) / 3600.0)
            ELSE NULL
        END                                                         AS avg_speed_mph,
        CASE
            WHEN trip_distance_miles > 0
            THEN total_amount / trip_distance_miles
            ELSE NULL
        END                                                         AS cost_per_mile,
        CASE
            WHEN total_amount > 0
            THEN tip_amount / total_amount
            ELSE 0
        END                                                         AS tip_percentage,

        -- Date parts for partitioning / BI
        DATE(pickup_datetime)                                       AS pickup_date,
        EXTRACT(HOUR FROM pickup_datetime)::BIGINT                 AS pickup_hour,
        TO_CHAR(pickup_datetime, 'Day')                            AS pickup_day_of_week,
        EXTRACT(MONTH FROM pickup_datetime)::BIGINT                AS pickup_month,
        EXTRACT(YEAR FROM pickup_datetime)::BIGINT                 AS pickup_year

    FROM cast_and_clean

),

deduplicated AS (

    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY pickup_datetime, dropoff_datetime, vendor_id, total_amount
            ORDER BY ingestion_id DESC
        ) AS rn
    FROM enrich

),

validated AS (

    SELECT *
    FROM deduplicated
    WHERE rn = 1
      -- Business rule filters
      AND total_amount  > 0
      AND fare_amount   >= 0
      AND trip_distance_miles >= 0
      AND passenger_count BETWEEN 0 AND 20
      AND trip_duration_mins BETWEEN 1 AND 360  -- 1 min to 6 hrs
      AND pickup_datetime < dropoff_datetime

)

SELECT
    -- Surrogate key
    {{ dbt_utils.generate_surrogate_key([
        'pickup_datetime',
        'dropoff_datetime',
        'vendor_id',
        'total_amount'
    ]) }}                                                           AS trip_id,

    ingestion_id,
    batch_id,
    ingested_at,
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    pickup_date,
    pickup_hour,
    pickup_day_of_week,
    pickup_month,
    pickup_year,
    passenger_count,
    trip_distance_miles,
    pickup_location_id,
    dropoff_location_id,
    rate_code_id,
    store_and_fwd_flag,
    payment_type_code,
    payment_type_label,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    airport_fee,
    total_amount,
    trip_duration_mins,
    avg_speed_mph,
    cost_per_mile,
    tip_percentage

FROM validated