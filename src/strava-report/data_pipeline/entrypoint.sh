#!/bin/bash
set -e

echo "=== 1. Running Strava Ingestion ==="
python get_strava_activities.py "$@"

echo "=== 2. Running dbt Transformation ==="
cd /app/dbt
dbt run --select activities_features --profiles-dir .

echo "=== Pipeline Completed Successfully ==="
