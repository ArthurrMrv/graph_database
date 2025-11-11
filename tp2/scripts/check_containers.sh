#!/bin/bash
set -e

# Check if running in container mode
CONTAINER_MODE=${1:-""}

if [ "$CONTAINER_MODE" = "container" ]; then
    API_URL=${API_URL:-http://app:8000}
    POSTGRES_HOST=${POSTGRES_HOST:-postgres}
    POSTGRES_PORT=${POSTGRES_PORT:-5432}
    POSTGRES_USER=${POSTGRES_USER:-app}
    POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-password}
    POSTGRES_DB=${POSTGRES_DB:-shop}
    ETL_PATH="/work/app/etl.py"
else
    API_URL=${API_URL:-http://127.0.0.1:8000}
    POSTGRES_HOST=${POSTGRES_HOST:-localhost}
    POSTGRES_PORT=${POSTGRES_PORT:-5432}
    POSTGRES_USER=${POSTGRES_USER:-app}
    POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-password}
    POSTGRES_DB=${POSTGRES_DB:-shop}
    ETL_PATH="./app/etl.py"
fi

export PGPASSWORD=$POSTGRES_PASSWORD

echo "=== Checking FastAPI health ==="
HEALTH_RESPONSE=$(curl -s "$API_URL/health" || echo "")
if [ -z "$HEALTH_RESPONSE" ]; then
    echo "✗ FastAPI health check failed"
    exit 1
fi

echo "$HEALTH_RESPONSE" | grep -q '"ok":true' && echo "✔ FastAPI health OK" || {
    echo "✗ FastAPI health check failed: $HEALTH_RESPONSE"
    exit 1
}

echo "$HEALTH_RESPONSE"
echo ""

echo "=== Checking Postgres ==="
echo "› Postgres: SELECT * FROM orders LIMIT 5;"
psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT * FROM orders LIMIT 5;" || {
    echo "✗ Orders query failed"
    exit 1
}
echo ""
echo "✔ Orders query OK"
echo ""

echo "› Postgres: SELECT now();"
psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT now();" || {
    echo "✗ now() query failed"
    exit 1
}
echo ""
echo "✔ now() query OK"
echo ""

echo "=== Running ETL ==="
echo "› ETL: python $ETL_PATH"
if [ "$CONTAINER_MODE" = "container" ]; then
    ETL_OUTPUT=$(cd /work && python "$ETL_PATH" 2>&1 | cat)
else
    ETL_OUTPUT=$(python "$ETL_PATH" 2>&1 | cat)
fi

echo "$ETL_OUTPUT"

if echo "$ETL_OUTPUT" | grep -q "ETL done."; then
    echo "✔ ETL output OK (ETL done.)"
else
    echo "✗ ETL output missing 'ETL done.'"
    exit 1
fi

echo ""
echo "=== All checks passed ==="

