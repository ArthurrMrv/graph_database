"""
FastAPI application for e-commerce graph recommendations.
Exposes endpoints for health checks and recommendation strategies.
"""

import os
from typing import Dict, Any, Optional
from fastapi import FastAPI, HTTPException
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="E-Commerce Graph Recommendations API")

# Neo4j connection
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

neo4j_driver = GraphDatabase.driver(
    NEO4J_URI,
    auth=(NEO4J_USER, NEO4J_PASSWORD),
)


def get_neo4j_health() -> bool:
    """Check if Neo4j is accessible."""
    try:
        with neo4j_driver.session() as session:
            session.run("RETURN 1")
        return True
    except Exception:
        return False


@app.get("/health")
async def health() -> Dict[str, Any]:
    """
    Health check endpoint.
    Returns the status of the API and Neo4j connection.
    """
    neo4j_ok = get_neo4j_health()
    return {
        "ok": neo4j_ok,
        "neo4j": "connected" if neo4j_ok else "disconnected",
    }


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "message": "E-Commerce Graph Recommendations API",
        "endpoints": {
            "health": "/health",
            "recommendations": "/recs",
        },
    }


@app.get("/recs")
async def get_recommendations(
    customer_id: Optional[str] = None,
    strategy: str = "co_occurrence",
    limit: int = 10,
):
    """
    Get product recommendations.
    
    Args:
        customer_id: Optional customer ID for personalized recommendations
        strategy: Recommendation strategy (co_occurrence, similarity, pagerank)
        limit: Maximum number of recommendations to return
    
    Returns:
        List of recommended products
    """
    if strategy == "co_occurrence":
        return await get_co_occurrence_recommendations(customer_id, limit)
    elif strategy == "similarity":
        return await get_similarity_recommendations(customer_id, limit)
    elif strategy == "pagerank":
        return await get_pagerank_recommendations(limit)
    else:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown strategy: {strategy}. Use: co_occurrence, similarity, pagerank",
        )


async def get_co_occurrence_recommendations(
    customer_id: Optional[str], limit: int
) -> Dict[str, Any]:
    """
    Get recommendations based on products that are frequently bought together.
    """
    query = """
    MATCH (p1:Product)<-[:CONTAINS]-(:Order)-[:CONTAINS]->(p2:Product)
    WHERE p1 <> p2
    WITH p2, count(*) AS co_count
    ORDER BY co_count DESC
    LIMIT $limit
    RETURN p2.id AS product_id, p2.name AS product_name, p2.price AS price, co_count AS score
    """
    
    with neo4j_driver.session() as session:
        result = session.run(query, {"limit": limit})
        recommendations = [
            {
                "product_id": record["product_id"],
                "product_name": record["product_name"],
                "price": record["price"],
                "score": record["score"],
            }
            for record in result
        ]
    
    return {
        "strategy": "co_occurrence",
        "customer_id": customer_id,
        "recommendations": recommendations,
    }


async def get_similarity_recommendations(
    customer_id: Optional[str], limit: int
) -> Dict[str, Any]:
    """
    Get recommendations based on products viewed/clicked by similar customers.
    """
    if not customer_id:
        # If no customer ID, return popular products
        query = """
        MATCH (c:Customer)-[:VIEWED|CLICKED|ADDED_TO_CART]->(p:Product)
        WITH p, count(DISTINCT c) AS customer_count
        ORDER BY customer_count DESC
        LIMIT $limit
        RETURN p.id AS product_id, p.name AS product_name, p.price AS price, customer_count AS score
        """
        params = {"limit": limit}
    else:
        # Find products liked by customers who liked similar products
        query = """
        MATCH (c1:Customer {id: $customer_id})-[:VIEWED|CLICKED|ADDED_TO_CART]->(p1:Product)
        MATCH (c2:Customer)-[:VIEWED|CLICKED|ADDED_TO_CART]->(p1)
        MATCH (c2)-[:VIEWED|CLICKED|ADDED_TO_CART]->(p2:Product)
        WHERE c1 <> c2 AND p1 <> p2
        WITH p2, count(DISTINCT c2) AS similar_customers
        ORDER BY similar_customers DESC
        LIMIT $limit
        RETURN p2.id AS product_id, p2.name AS product_name, p2.price AS price, similar_customers AS score
        """
        params = {"customer_id": customer_id, "limit": limit}
    
    with neo4j_driver.session() as session:
        result = session.run(query, params)
        recommendations = [
            {
                "product_id": record["product_id"],
                "product_name": record["product_name"],
                "price": record["price"],
                "score": record["score"],
            }
            for record in result
        ]
    
    return {
        "strategy": "similarity",
        "customer_id": customer_id,
        "recommendations": recommendations,
    }


async def get_pagerank_recommendations(limit: int) -> Dict[str, Any]:
    """
    Get recommendations based on PageRank of products in the graph.
    Note: This requires GDS library and a named graph to be created.
    For now, returns products by order frequency.
    """
    query = """
    MATCH (p:Product)<-[:CONTAINS]-(:Order)
    WITH p, count(*) AS order_count
    ORDER BY order_count DESC
    LIMIT $limit
    RETURN p.id AS product_id, p.name AS product_name, p.price AS price, order_count AS score
    """
    
    with neo4j_driver.session() as session:
        result = session.run(query, {"limit": limit})
        recommendations = [
            {
                "product_id": record["product_id"],
                "product_name": record["product_name"],
                "price": record["price"],
                "score": record["score"],
            }
            for record in result
        ]
    
    return {
        "strategy": "pagerank",
        "recommendations": recommendations,
    }


@app.on_event("shutdown")
async def shutdown():
    """Close Neo4j driver on shutdown."""
    neo4j_driver.close()

