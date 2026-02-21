-- Singular test: Every silver trip date should appear in gold summary
-- Detects data loss in the silver → gold transformation
-- File: tests/assert_gold_coverage.sql

WITH silver_dates AS (
    SELECT DISTINCT pickup_date
    FROM {{ ref('cleaned_taxi_trips') }}
),

gold_dates AS (
    SELECT DISTINCT pickup_date
    FROM {{ ref('daily_trip_summary') }}
)

SELECT s.pickup_date
FROM silver_dates s
LEFT JOIN gold_dates g ON s.pickup_date = g.pickup_date
WHERE g.pickup_date IS NULL
