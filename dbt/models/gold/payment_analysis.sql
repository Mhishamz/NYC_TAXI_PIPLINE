{{
  config(
    materialized='table',
    schema='gold',
    alias='payment_analysis',
    tags=['gold', 'business', 'powerbi']
  )
}}

/*
  Gold Layer: payment_analysis
  ─────────────────────────────
  Payment type breakdown with tipping behavior analysis.
  Drives donut charts and tip behavior dashboards in Power BI.
*/

WITH base AS (

    SELECT *
    FROM {{ ref('cleaned_taxi_trips') }}

),

payment_agg AS (

    SELECT
        payment_type_code,
        payment_type_label,
        COUNT(*)                                           AS trip_count,
        ROUND(SUM(total_amount)::NUMERIC, 2)              AS total_revenue,
        ROUND(AVG(total_amount)::NUMERIC, 2)              AS avg_fare,
        ROUND(SUM(tip_amount)::NUMERIC, 2)                AS total_tips,
        ROUND(AVG(tip_amount)::NUMERIC, 2)                AS avg_tip,
        ROUND(AVG(tip_percentage)::NUMERIC, 4)            AS avg_tip_rate,
        COUNT(*) FILTER (WHERE tip_amount > 0)            AS tipped_trips,
        ROUND(AVG(trip_distance_miles)::NUMERIC, 2)       AS avg_distance,
        ROUND(AVG(trip_duration_mins)::NUMERIC, 2)        AS avg_duration_mins
    FROM base
    GROUP BY payment_type_code, payment_type_label

),

totals AS (
    SELECT SUM(trip_count) AS grand_total_trips,
           SUM(total_revenue) AS grand_total_revenue
    FROM payment_agg
)

SELECT
    p.*,
    ROUND((p.trip_count::NUMERIC / t.grand_total_trips) * 100, 2) AS pct_of_total_trips,
    ROUND((p.total_revenue::NUMERIC / t.grand_total_revenue) * 100, 2) AS pct_of_total_revenue,
    ROUND((p.tipped_trips::NUMERIC / NULLIF(p.trip_count, 0)) * 100, 2) AS tip_frequency_pct,
    NOW() AS last_updated_at
FROM payment_agg p
CROSS JOIN totals t
ORDER BY trip_count DESC
