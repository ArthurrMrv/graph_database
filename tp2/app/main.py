#!/usr/bin/env python3
"""
FastAPI application for e-commerce graph recommendations.
Provides REST API endpoints for querying Neo4j graph database.
"""

import os
from typing import Dict, Any

from fastapi import FastAPI, HTTPException
from neo4j import GraphDatabase


app = FastAPI(
    title="E-Commerce Graph Recommendations API",
    description="Graph-based recommendation engine using Neo4j",
    version="1.0.0"
)


# Neo4j connection
def get_neo4j_driver():
    """Get Neo4j driver instance."""
    uri = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "password")
    return GraphDatabase.driver(uri, auth=(user, password))


@app.get("/health")
async def health_check() -> Dict[str, Any]:
    """
    Health check endpoint.
    Returns the status of the API and Neo4j connection.
    """
    try:
        driver = get_neo4j_driver()
        with driver.session() as session:
            result = session.run("RETURN 1 AS health")
            result.single()
        driver.close()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/")
async def root() -> Dict[str, str]:
    """Root endpoint with API information."""
    return {
        "message": "E-Commerce Graph Recommendations API",
        "version": "1.0.0",
        "endpoints": {
            "health": "/health",
            "docs": "/docs"
        }
    }


@app.get("/stats")
async def get_stats() -> Dict[str, Any]:
    """
    Get database statistics.
    Returns counts of nodes and relationships.
    """
    try:
        driver = get_neo4j_driver()
        with driver.session() as session:
            # Count nodes
            customers = session.run("MATCH (c:Customer) RETURN count(c) AS count").single()["count"]
            products = session.run("MATCH (p:Product) RETURN count(p) AS count").single()["count"]
            orders = session.run("MATCH (o:Order) RETURN count(o) AS count").single()["count"]
            categories = session.run("MATCH (cat:Category) RETURN count(cat) AS count").single()["count"]
            
            # Count relationships
            placed = session.run("MATCH ()-[r:PLACED]->() RETURN count(r) AS count").single()["count"]
            contains = session.run("MATCH ()-[r:CONTAINS]->() RETURN count(r) AS count").single()["count"]
            in_category = session.run("MATCH ()-[r:IN_CATEGORY]->() RETURN count(r) AS count").single()["count"]
            
            # Count event relationships
            viewed = session.run("MATCH ()-[r:VIEWED]->() RETURN count(r) AS count").single()["count"]
            clicked = session.run("MATCH ()-[r:CLICKED]->() RETURN count(r) AS count").single()["count"]
            added = session.run("MATCH ()-[r:ADDED_TO_CART]->() RETURN count(r) AS count").single()["count"]
        
        driver.close()
        
        return {
            "nodes": {
                "customers": customers,
                "products": products,
                "orders": orders,
                "categories": categories,
                "total": customers + products + orders + categories
            },
            "relationships": {
                "placed": placed,
                "contains": contains,
                "in_category": in_category,
                "viewed": viewed,
                "clicked": clicked,
                "added_to_cart": added,
                "total": placed + contains + in_category + viewed + clicked + added
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
