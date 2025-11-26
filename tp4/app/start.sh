#!/bin/bash

# Wait for Neo4j to be ready
echo "Waiting for Neo4j..."
sleep 15

echo "Starting TP4 Graph ML Pipeline..."
python main.py
