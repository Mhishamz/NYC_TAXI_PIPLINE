{{
  config(
    materialized='table',
    schema='gold',
    alias='daily_trip_summary',
    tags=['gold', 'business', 'powerbi'],
    indexes=[
      {'columns': ['pickup_date'], 'type': 'btree'},
    ]
  )
}}

/*
  Gold Layer: daily_trip_summary
  ─────────────────────────────────
  Business-ready daily aggregation of NYC taxi trips.
  Directly consumed by Power BI dashboards.
  Metrics: trips, revenue, tips, distances, duration, passenger counts.
*/

WITH base AS (

    SELECT *
    FROM {{ ref('cleaned_taxi_trips') }}

),

daily_agg AS (

    SELECT
        pickup_date,
        pickup_year,
        pickup_month,
        vendor_id,
        payment_type_label,

        -- Volume
        COUNT(*)                                                AS total_trips,
        SUM(passenger_count)                                    AS total_passengers,

        -- Revenue
        ROUND(SUM(total_amount)::NUMERIC, 2)                   AS total_revenue,
        ROUND(AVG(total_amount)::NUMERIC, 2)                   AS avg_fare_per_trip,
        ROUND(SUM(fare_amount)::NUMERIC, 2)                    AS total_base_fare,
        ROUND(SUM(tip_amount)::NUMERIC, 2)                     AS total_tips,
        ROUND(AVG(tip_percentage)::NUMERIC, 4)                 AS avg_tip_rate,
        ROUND(SUM(tolls_amount)::NUMERIC, 2)                   AS total_tolls,
        ROUND(SUM(congestion_surcharge)::NUMERIC, 2)           AS total_congestion_surcharge,

        -- Distance
        ROUND(SUM(trip_distance_miles)::NUMERIC, 2)            AS total_distance_miles,
        ROUND(AVG(trip_distance_miles)::NUMERIC, 2)            AS avg_distance_miles,

        -- Duration
        ROUND(AVG(trip_duration_mins)::NUMERIC, 2)             AS avg_duration_mins,
        ROUND(SUM(trip_duration_mins)::NUMERIC, 2)             AS total_duration_mins,

        -- Speed & cost efficiency
        ROUND(AVG(avg_speed_mph)::NUMERIC, 2)                  AS avg_speed_mph,
        ROUND(AVG(cost_per_mile)::NUMERIC, 2)                  AS avg_cost_per_mile,

        -- Peak hour (mode)
        MODE() WITHIN GROUP (ORDER BY pickup_hour)             AS peak_pickup_hour

    FROM base
    GROUP BY
        pickup_date,
        pickup_year,
        pickup_month,
        vendor_id,
        payment_type_label

)

SELECT
    -- Surrogate key
    {{ dbt_utils.generate_surrogate_key([
        'pickup_date',
        'vendor_id',
        'payment_type_label'
    ]) }}                                                       AS summary_id,

    pickup_date,
    pickup_year,
    pickup_month,
    TO_CHAR(pickup_date, 'Month')                              AS month_name,
    TO_CHAR(pickup_date, 'Day')                                AS day_of_week,
    vendor_id,
    CASE vendor_id
        WHEN 1 THEN 'Creative Mobile Technologies'
        WHEN 2 THEN 'VeriFone Inc.'
        ELSE        'Unknown Vendor'
    END                                                         AS vendor_name,
    payment_type_label,
    total_trips,
    total_passengers,
    total_revenue,
    avg_fare_per_trip,
    total_base_fare,
    total_tips,
    avg_tip_rate,
    total_tolls,
    total_congestion_surcharge,
    total_distance_miles,
    avg_distance_miles,
    avg_duration_mins,
    total_duration_mins,
    avg_speed_mph,
    avg_cost_per_mile,
    peak_pickup_hour,
    NOW()                                                       AS last_updated_at

FROM daily_agg
