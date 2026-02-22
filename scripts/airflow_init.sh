#!/bin/bash
set -e

echo ">>> [1/3] Waiting for Airflow metadata DB..."
until airflow db check 2>/dev/null; do
  echo "    DB not ready, retrying in 5s..."
  sleep 5
done

echo ">>> [2/3] Running DB migrations..."
airflow db migrate

echo ">>> [3/3] Creating admin user..."
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin || echo "User already exists, skipping."

echo ">>> Init complete."