#!/bin/bash

BASE_URL="http://localhost:8000"

echo "Testing Root Endpoint..."
curl -s "$BASE_URL/" | jq .

echo -e "\n\nTesting Data Load (this might take a while)..."
curl -X POST -s "$BASE_URL/load-data" | jq .

echo -e "\n\nTesting Projection Creation..."
curl -X POST -s "$BASE_URL/create-projections" | jq .

echo -e "\n\nTesting PageRank..."
curl -s "$BASE_URL/analysis/pagerank" | jq .

echo -e "\n\nTesting Question 1 (5 random users)..."
curl -s "$BASE_URL/questions/1" | jq .
