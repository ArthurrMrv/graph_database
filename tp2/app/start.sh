#!/bin/bash
set -e

echo "🚀 Starting E-Commerce Graph Recommendations API..."

# Run ETL on first startup (optional - can be run manually)
if [ ! -f /tmp/etl_completed ]; then
    echo "📊 Running ETL for the first time..."
    python etl.py
    touch /tmp/etl_completed
    echo "✅ ETL completed"
fi

# Start FastAPI server with hot reload
echo "🌐 Starting uvicorn server..."
exec uvicorn main:app --host 0.0.0.0 --port 8000 --reload
