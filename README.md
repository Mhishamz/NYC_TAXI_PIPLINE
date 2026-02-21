# 🚕 NYC Taxi Data Pipeline

> **Production-grade ELT pipeline**: NYC Open Data API → PostgreSQL (Medallion Architecture) → Power BI  
> Orchestrated with **Apache Airflow**, transformed with **dbt**, containerized with **Docker Compose**

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        NYC Taxi ELT Pipeline                        │
│                                                                     │
│  NYC Open Data API                                                  │
│       │                                                             │
│       ▼                                                             │
│  ┌──────────┐    ┌─────────────────────────────────────────────┐   │
│  │ Airflow  │───▶│          PostgreSQL Data Warehouse           │   │
│  │Scheduler │    │                                             │   │
│  │  (DAG)   │    │  bronze.*  │  silver.*  │     gold.*        │   │
│  └──────────┘    │  (raw)     │  (cleaned) │  (aggregated)     │   │
│       │          └─────────────────────────────────────────────┘   │
│       │                    ▲                                        │
│       └──── dbt ───────────┘                                        │
│              (bronze→silver→gold transformations + tests)           │
│                                                                     │
│  metadata.pipeline_runs (audit log)          Power BI ◀── gold.*   │
└─────────────────────────────────────────────────────────────────────┘
```

### Medallion Architecture

| Layer      | Schema     | Table                     | Description                          |
|------------|------------|---------------------------|--------------------------------------|
| 🥉 Bronze  | `bronze`   | `raw_taxi_trips`          | Raw strings from API, append-only    |
| 🥈 Silver  | `silver`   | `cleaned_taxi_trips`      | Typed, validated, deduplicated       |
| 🥇 Gold    | `gold`     | `daily_trip_summary`      | Daily business aggregations          |
| 🥇 Gold    | `gold`     | `hourly_demand_heatmap`   | Hour × DOW demand matrix             |
| 🥇 Gold    | `gold`     | `payment_analysis`        | Payment type breakdown               |
| 📋 Meta    | `metadata` | `pipeline_runs`           | Pipeline audit log                   |

---

## 🐳 Services

| Service            | Port   | Description                          |
|--------------------|--------|--------------------------------------|
| Airflow Webserver  | 8080   | DAG monitoring & triggering          |
| postgres-airflow   | —      | Airflow metadata (internal)          |
| postgres-dw        | 5433   | NYC Taxi Data Warehouse              |
| Adminer            | 8081   | Database GUI (dev use)               |

---

## 🚀 Quick Start

### 1. Clone & configure

```bash
git clone <repo-url>
cd nyc-taxi-pipeline
cp .env.example .env   # edit if needed
```

### 2. Start the stack

```bash
docker compose up -d --build
```

### 3. Register Airflow connections

```bash
chmod +x scripts/setup_connections.sh
./scripts/setup_connections.sh
```

### 4. Install dbt packages & test connection

```bash
# One-off dbt container run
docker compose --profile dbt run dbt deps
docker compose --profile dbt run dbt debug
```

### 5. Trigger the pipeline

- Open **http://localhost:8080** (admin / admin)
- Enable the `nyc_taxi_pipeline` DAG
- Click **Trigger DAG**

### 6. Connect Power BI

See [`powerbi/POWERBI_SETUP.md`](powerbi/POWERBI_SETUP.md) for full instructions.  
Short version: connect to `localhost:5433 / nyc_taxi_dw` → load `gold.*` tables.

---

## 📁 Project Structure

```
nyc-taxi-pipeline/
├── docker-compose.yml              # All services
├── .env                            # Environment variables
│
├── airflow/
│   ├── dags/
│   │   └── nyc_taxi_pipeline.py   # Main pipeline DAG
│   ├── logs/                       # Airflow task logs
│   ├── plugins/
│   └── config/airflow.cfg
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── packages.yml               # dbt_utils
│   └── models/
│       ├── bronze/
│       │   └── sources.yml        # Source definitions + tests
│       ├── silver/
│       │   ├── cleaned_taxi_trips.sql
│       │   └── schema.yml         # Column-level tests
│       └── gold/
│           ├── daily_trip_summary.sql
│           ├── hourly_demand_heatmap.sql
│           ├── payment_analysis.sql
│           └── schema.yml
│   └── tests/                     # Custom singular tests
│       ├── assert_dropoff_after_pickup.sql
│       ├── assert_total_amount_gte_fare.sql
│       └── assert_gold_coverage.sql
│
├── postgres/
│   └── init/
│       └── 01_init_schemas.sql    # Schema + table DDL
│
├── powerbi/
│   └── POWERBI_SETUP.md           # Connection + dashboard guide
│
└── scripts/
    └── setup_connections.sh       # Post-startup Airflow config
```

---

## 🔁 DAG Flow

```
log_pipeline_start
      │
      ▼
 ingest_data            ← Fetches NYC API, loads to bronze.*
      │
      ▼
validate_bronze         ← Null checks, row counts
      │
      ▼
dbt_transform_silver    ← bronze → silver (cast, clean, dedupe)
      │
      ▼
dbt_transform_gold      ← silver → gold (aggregate for BI)
      │
      ▼
   dbt_test             ← Schema tests + custom singular tests
      │
      ▼
dbt_generate_docs       ← Refreshes dbt catalog
      │
   ┌──┴──┐
   ▼     ▼
 log_  log_
success failure
```

---

## 🧪 dbt Tests

### Generic (schema.yml)
- `unique`, `not_null` on all key columns
- `accepted_values` on vendor_id, payment_type
- `dbt_utils.expression_is_true` for numeric range checks (fares > 0, tip rate 0–1)

### Singular (tests/)
| Test                              | Assertion                                      |
|-----------------------------------|------------------------------------------------|
| `assert_dropoff_after_pickup`     | No trips with negative duration               |
| `assert_total_amount_gte_fare`    | Total ≥ base fare always                       |
| `assert_gold_coverage`            | Every silver date exists in gold              |

---

## 📊 Silver Layer Transformations

| Raw Column              | Silver Column            | Transformation                        |
|-------------------------|--------------------------|---------------------------------------|
| `passenger_count` (str) | `passenger_count` (INT)  | Cast + null filter                    |
| `fare_amount` (str)     | `fare_amount` (NUMERIC)  | Cast + validate ≥ 0                   |
| `payment_type` (str)    | `payment_type_label`     | Code → label map                      |
| —                       | `trip_duration_mins`     | EXTRACT(EPOCH from dropoff-pickup)/60 |
| —                       | `avg_speed_mph`          | distance / duration                   |
| —                       | `cost_per_mile`          | total / distance                      |
| —                       | `tip_percentage`         | tip / total                           |
| —                       | `trip_id`                | SHA surrogate key (dbt_utils)         |

---

## 📈 Monitoring & Logging

- **Pipeline runs** logged in `metadata.pipeline_runs` with row counts and duration
- **Airflow task logs** persisted in `airflow/logs/` (JSON format from dbt)
- **Docker logs**: `docker compose logs -f airflow-scheduler`
- **Adminer**: http://localhost:8081 for live SQL queries on all schemas

---

## 🔧 Configuration

| Variable        | Default                                           | Description          |
|-----------------|---------------------------------------------------|----------------------|
| `PAGE_SIZE`     | 1000                                              | Records per API page |
| `MAX_PAGES`     | 10                                                | Max pages per run    |
| `NYC_TAXI_URL`  | https://data.cityofnewyork.us/resource/qp3b-zxtp.json | Source URL    |
| Schedule        | `0 2 * * *`                                       | Daily 02:00 UTC      |

---

## 🛠️ Common Commands

```bash
# View all logs
docker compose logs -f

# Run dbt manually
docker compose --profile dbt run dbt run --select silver
docker compose --profile dbt run dbt test

# Inspect warehouse
docker compose exec postgres-dw psql -U dwuser -d nyc_taxi_dw \
  -c "SELECT COUNT(*) FROM silver.cleaned_taxi_trips;"

# Check pipeline metadata
docker compose exec postgres-dw psql -U dwuser -d nyc_taxi_dw \
  -c "SELECT * FROM metadata.pipeline_runs ORDER BY run_id DESC LIMIT 5;"

# Stop everything
docker compose down -v   # -v removes volumes (fresh start)
```

---

## 🔒 Production Hardening Checklist

- [ ] Move credentials to **Docker Secrets** or **Vault**
- [ ] Enable **remote logging** (S3/GCS) in `airflow.cfg`
- [ ] Add **AlertManager** or email alerts on DAG failure
- [ ] Set `MAX_PAGES` dynamically based on last ingestion watermark
- [ ] Add **Incremental loads** (filter by `tpep_pickup_datetime > last_run`)
- [ ] Enable **dbt Cloud** for hosted docs + job scheduling
- [ ] Add **Great Expectations** for advanced data quality profiling
# NYC_TAXI_PIPLINE
