-- Singular test: No trips should have dropoff BEFORE pickup
-- File: tests/assert_dropoff_after_pickup.sql

SELECT
    trip_id,
    pickup_datetime,
    dropoff_datetime,
    trip_duration_mins
FROM {{ ref('cleaned_taxi_trips') }}
WHERE dropoff_datetime <= pickup_datetime
   OR trip_duration_mins <= 0
