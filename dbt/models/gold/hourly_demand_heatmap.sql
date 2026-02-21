{{
  config(
    materialized='table',
    schema='gold',
    alias='hourly_demand_heatmap',
    tags=['gold', 'business', 'powerbi']
  )
}}

/*
  Gold Layer: hourly_demand_heatmap
  ─────────────────────────────────
  Hour-of-day × Day-of-week demand matrix for Power BI heatmap visuals.
*/

WITH base AS (

    SELECT
        pickup_hour,
        pickup_day_of_week,
        pickup_date,
        EXTRACT(DOW FROM pickup_datetime)::INTEGER AS day_of_week_num,
        total_amount,
        trip_distance_miles,
        trip_duration_mins
    FROM {{ ref('cleaned_taxi_trips') }}

)

SELECT
    pickup_hour,
    pickup_day_of_week,
    day_of_week_num,
    COUNT(*)                                          AS trip_count,
    ROUND(AVG(total_amount)::NUMERIC, 2)             AS avg_fare,
    ROUND(AVG(trip_distance_miles)::NUMERIC, 2)      AS avg_distance,
    ROUND(AVG(trip_duration_mins)::NUMERIC, 2)       AS avg_duration_mins,
    ROUND(SUM(total_amount)::NUMERIC, 2)             AS total_revenue,
    NOW()                                             AS last_updated_at

FROM base
GROUP BY pickup_hour, pickup_day_of_week, day_of_week_num
ORDER BY day_of_week_num, pickup_hour
