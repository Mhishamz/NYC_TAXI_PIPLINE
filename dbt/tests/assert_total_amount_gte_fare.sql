-- Singular test: Total amount should always be >= fare_amount
-- (total = fare + tips + surcharges, never less than base fare)
-- File: tests/assert_total_amount_gte_fare.sql

SELECT
    trip_id,
    fare_amount,
    total_amount,
    tip_amount,
    tolls_amount
FROM {{ ref('cleaned_taxi_trips') }}
WHERE total_amount < fare_amount
  AND total_amount > 0
