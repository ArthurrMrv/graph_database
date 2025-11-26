#!/bin/bash

# Wait for Neo4j to be ready (double check, though docker depends_on handles most of it)
echo "Waiting for Neo4j..."
# Simple sleep to be safe, or we could loop check the port
sleep 10

echo "Starting FastAPI app..."
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
