# Power BI Connection Guide

## Connecting Power BI Desktop to the NYC Taxi Gold Layer

### Prerequisites
- Power BI Desktop installed (free from Microsoft)
- Docker pipeline running (`docker compose up -d`)

---

## Step 1: Open Power BI Desktop → Get Data

1. Click **Home → Get Data → PostgreSQL database**
2. Enter connection details:

| Field    | Value           |
|----------|-----------------|
| Server   | `localhost:5433` |
| Database | `nyc_taxi_dw`   |

3. Credentials: **Database** auth
   - Username: `dwuser`
   - Password: `dwpassword`

---

## Step 2: Select Gold Tables

In the Navigator, select these tables from the **gold** schema:

| Table                   | Description                       |
|-------------------------|-----------------------------------|
| `daily_trip_summary`    | Main time-series & revenue data   |
| `hourly_demand_heatmap` | Hour × Day demand matrix          |
| `payment_analysis`      | Payment method breakdown          |

Click **Load** (or **Transform Data** to preview first).

---

## Step 3: Recommended Visuals

### Dashboard 1 — Revenue Overview
- **Line Chart**: `pickup_date` (X) vs `total_revenue` (Y), color by `vendor_name`
- **KPI Cards**: Total Revenue | Total Trips | Avg Fare | Avg Tip Rate
- **Donut Chart**: `payment_type_label` by `pct_of_total_trips`

### Dashboard 2 — Demand Analysis
- **Matrix Heatmap**: Rows = `pickup_day_of_week`, Columns = `pickup_hour`, Values = `trip_count`
  → Use conditional formatting (color scale) for heatmap effect
- **Bar Chart**: Top hours by `avg_fare` using `hourly_demand_heatmap`

### Dashboard 3 — Tipping Behavior
- **Clustered Bar**: `payment_type_label` vs `avg_tip_rate` and `tip_frequency_pct`
- **Scatter Plot**: `avg_distance` (X) vs `avg_tip` (Y), size = `trip_count`

---

## Step 4: Set Auto-Refresh (Optional)
- **Power BI Service**: Publish report → Dataset Settings → Scheduled Refresh
- Connect via **On-premises data gateway** if pipeline runs locally

---

## DAX Measure Examples

```dax
-- Revenue Growth MoM
Revenue MoM Growth =
VAR CurrentMonth = SUM(daily_trip_summary[total_revenue])
VAR PrevMonth = CALCULATE(
    SUM(daily_trip_summary[total_revenue]),
    DATEADD(daily_trip_summary[pickup_date], -1, MONTH)
)
RETURN DIVIDE(CurrentMonth - PrevMonth, PrevMonth)

-- Average Revenue per Passenger
Avg Revenue per Pax =
DIVIDE(
    SUM(daily_trip_summary[total_revenue]),
    SUM(daily_trip_summary[total_passengers])
)
```
