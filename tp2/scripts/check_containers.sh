#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Check if running in container mode
if [ "$CONTAINER_MODE" = "true" ]; then
    API_HOST="app"
    POSTGRES_HOST="postgres"
else
    API_HOST="127.0.0.1"
    POSTGRES_HOST="localhost"
fi

echo -e "${BLUE}🧪 Running E-Commerce Graph System Tests${NC}\n"

# Test 1: FastAPI Health Check
echo -e "${BLUE}› FastAPI health check${NC}"
HEALTH_RESPONSE=$(curl -s http://${API_HOST}:8000/health)
if echo "$HEALTH_RESPONSE" | grep -q '"ok"'; then
    echo -e "${GREEN}✔ FastAPI health OK${NC}"
    echo "$HEALTH_RESPONSE"
else
    echo -e "${RED}✖ FastAPI health check failed${NC}"
    echo "$HEALTH_RESPONSE"
    exit 1
fi

echo ""

# Test 2: Postgres - Check orders table
echo -e "${BLUE}› Postgres: SELECT * FROM orders LIMIT 5;${NC}"
if [ "$CONTAINER_MODE" = "true" ]; then
    ORDERS_RESULT=$(PGPASSWORD=apppass psql -h $POSTGRES_HOST -U app -d shop -c "SELECT * FROM orders LIMIT 5;")
else
    ORDERS_RESULT=$(docker compose exec -T postgres psql -U app -d shop -c "SELECT * FROM orders LIMIT 5;")
fi

echo "$ORDERS_RESULT"
if echo "$ORDERS_RESULT" | grep -q "O1"; then
    echo -e "${GREEN}✔ Orders query OK${NC}"
else
    echo -e "${RED}✖ Orders query failed${NC}"
    exit 1
fi

echo ""

# Test 3: Postgres - Check current time
echo -e "${BLUE}› Postgres: SELECT now();${NC}"
if [ "$CONTAINER_MODE" = "true" ]; then
    NOW_RESULT=$(PGPASSWORD=apppass psql -h $POSTGRES_HOST -U app -d shop -c "SELECT now();")
else
    NOW_RESULT=$(docker compose exec -T postgres psql -U app -d shop -c "SELECT now();")
fi

echo "$NOW_RESULT"
if echo "$NOW_RESULT" | grep -q "20"; then
    echo -e "${GREEN}✔ now() query OK${NC}"
else
    echo -e "${RED}✖ now() query failed${NC}"
    exit 1
fi

echo ""

# Test 4: Run ETL
echo -e "${BLUE}› ETL: python /work/app/etl.py${NC}"
if [ "$CONTAINER_MODE" = "true" ]; then
    ETL_OUTPUT=$(python /work/app/etl.py 2>&1 | cat)
else
    ETL_OUTPUT=$(docker compose exec -T app python /work/app/etl.py 2>&1 | cat)
fi

echo "$ETL_OUTPUT"
if echo "$ETL_OUTPUT" | grep -q "ETL done"; then
    echo -e "${GREEN}✔ ETL output OK (ETL done.)${NC}"
else
    echo -e "${RED}✖ ETL did not complete successfully${NC}"
    exit 1
fi

echo ""
echo -e "${GREEN}✅ All tests passed!${NC}"
