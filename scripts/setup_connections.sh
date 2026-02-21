#!/usr/bin/env bash
# ============================================================
# setup_connections.sh
# Run this ONCE after docker-compose up to register Airflow
# connections for the data warehouse.
# ============================================================

set -e

echo "⏳ Waiting for Airflow webserver to be ready..."
until curl -s http://localhost:8080/health | grep -q '"metadatabase":{"status":"healthy"}'; do
  sleep 5
  echo "   Still waiting..."
done
echo "✅ Airflow is healthy!"

echo ""
echo "📡 Registering postgres_dw connection in Airflow..."
docker compose exec airflow-webserver airflow connections add postgres_dw \
  --conn-type postgres \
  --conn-host postgres-dw \
  --conn-port 5432 \
  --conn-login dwuser \
  --conn-password dwpassword \
  --conn-schema nyc_taxi_dw \
  --conn-description "NYC Taxi Data Warehouse (Medallion Architecture)"

echo "✅ Connection 'postgres_dw' registered."
echo ""
echo "🚀 Setup complete! Open http://localhost:8080 (admin/admin)"
echo "   Trigger the 'nyc_taxi_pipeline' DAG to start."
