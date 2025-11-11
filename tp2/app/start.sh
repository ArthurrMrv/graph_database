#!/bin/bash
set -e

echo "Installing dependencies..."
pip install -q -r requirements.txt

echo "Starting FastAPI server..."
exec uvicorn main:app --host 0.0.0.0 --port 8000 --reload

