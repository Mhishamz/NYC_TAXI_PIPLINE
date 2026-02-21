-- ============================================================
-- NYC Taxi Data Warehouse - Medallion Architecture Init
-- ============================================================

-- Bronze Layer: Raw ingested data (as-is from source)
CREATE SCHEMA IF NOT EXISTS bronze;

-- Silver Layer: Cleaned, deduplicated, typed data
CREATE SCHEMA IF NOT EXISTS silver;

-- Gold Layer: Aggregated, business-ready data
CREATE SCHEMA IF NOT EXISTS gold;

-- Metadata schema for pipeline tracking
CREATE SCHEMA IF NOT EXISTS metadata;

-- ─────────────────────────────────────────────
-- Metadata: Pipeline Run Logs
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS metadata.pipeline_runs (
    run_id          SERIAL PRIMARY KEY,
    dag_id          VARCHAR(255)  NOT NULL,
    task_id         VARCHAR(255)  NOT NULL,
    execution_date  TIMESTAMP     NOT NULL,
    status          VARCHAR(50)   NOT NULL,  -- 'running', 'success', 'failed'
    rows_ingested   INTEGER       DEFAULT 0,
    rows_inserted   INTEGER       DEFAULT 0,
    rows_updated    INTEGER       DEFAULT 0,
    error_message   TEXT,
    started_at      TIMESTAMP     DEFAULT NOW(),
    completed_at    TIMESTAMP,
    duration_secs   NUMERIC(10,2)
);

-- ─────────────────────────────────────────────
-- Bronze: Raw NYC Taxi Trips
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bronze.raw_taxi_trips (
    ingestion_id        BIGSERIAL PRIMARY KEY,
    vendor_id           VARCHAR(10),
    tpep_pickup_datetime  VARCHAR(50),
    tpep_dropoff_datetime VARCHAR(50),
    passenger_count     VARCHAR(10),
    trip_distance       VARCHAR(20),
    rate_code_id        VARCHAR(10),
    store_and_fwd_flag  VARCHAR(5),
    pu_location_id      VARCHAR(10),
    do_location_id      VARCHAR(10),
    payment_type        VARCHAR(10),
    fare_amount         VARCHAR(20),
    extra               VARCHAR(20),
    mta_tax             VARCHAR(20),
    tip_amount          VARCHAR(20),
    tolls_amount        VARCHAR(20),
    improvement_surcharge VARCHAR(20),
    total_amount        VARCHAR(20),
    congestion_surcharge  VARCHAR(20),
    airport_fee         VARCHAR(20),
    -- Audit columns
    source_url          TEXT,
    ingested_at         TIMESTAMP   DEFAULT NOW(),
    batch_id            VARCHAR(100),
    pipeline_run_id     INTEGER     REFERENCES metadata.pipeline_runs(run_id)
);

CREATE INDEX IF NOT EXISTS idx_raw_taxi_ingested_at 
    ON bronze.raw_taxi_trips(ingested_at);
CREATE INDEX IF NOT EXISTS idx_raw_taxi_batch_id 
    ON bronze.raw_taxi_trips(batch_id);

-- ─────────────────────────────────────────────
-- Grant permissions
-- ─────────────────────────────────────────────
GRANT ALL PRIVILEGES ON SCHEMA bronze TO dwuser;
GRANT ALL PRIVILEGES ON SCHEMA silver TO dwuser;
GRANT ALL PRIVILEGES ON SCHEMA gold   TO dwuser;
GRANT ALL PRIVILEGES ON SCHEMA metadata TO dwuser;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA bronze TO dwuser;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA metadata TO dwuser;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA bronze TO dwuser;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA metadata TO dwuser;

-- Future tables auto-grant
ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT ALL ON TABLES TO dwuser;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold   GRANT ALL ON TABLES TO dwuser;
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT ALL ON TABLES TO dwuser;
ALTER DEFAULT PRIVILEGES IN SCHEMA metadata GRANT ALL ON TABLES TO dwuser;

\echo 'NYC Taxi DW initialized: bronze, silver, gold, metadata schemas ready.'
