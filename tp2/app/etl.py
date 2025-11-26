#!/usr/bin/env python3
"""
ETL script to migrate data from PostgreSQL to Neo4j.
Transforms relational e-commerce data into a graph structure.
"""

import os
import sys
import time
from pathlib import Path
from typing import Iterator, List, Any

import psycopg2
import pandas as pd
from neo4j import GraphDatabase


def wait_for_postgres(max_retries: int = 30, delay: int = 2) -> None:
    """Wait for PostgreSQL to be ready."""
    print("⏳ Waiting for PostgreSQL...")
    
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(
                host=os.getenv("POSTGRES_HOST", "postgres"),
                port=int(os.getenv("POSTGRES_PORT", "5432")),
                user=os.getenv("POSTGRES_USER", "app"),
                password=os.getenv("POSTGRES_PASSWORD", "apppass"),
                database=os.getenv("POSTGRES_DB", "shop"),
                sslmode="disable"
            )
            conn.close()
            print("✅ PostgreSQL is ready!")
            return
        except psycopg2.OperationalError:
            if attempt < max_retries - 1:
                time.sleep(delay)
            else:
                raise Exception("PostgreSQL not available after max retries")


def wait_for_neo4j(max_retries: int = 30, delay: int = 2) -> None:
    """Wait for Neo4j to be ready."""
    print("⏳ Waiting for Neo4j...")
    
    uri = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "password")
    
    for attempt in range(max_retries):
        try:
            driver = GraphDatabase.driver(uri, auth=(user, password))
            with driver.session() as session:
                session.run("RETURN 1")
            driver.close()
            print("✅ Neo4j is ready!")
            return
        except Exception:
            if attempt < max_retries - 1:
                time.sleep(delay)
            else:
                raise Exception("Neo4j not available after max retries")


def run_cypher(driver: GraphDatabase.driver, query: str, parameters: dict = None) -> None:
    """Execute a single Cypher query."""
    with driver.session() as session:
        session.run(query, parameters or {})


def run_cypher_file(driver: GraphDatabase.driver, filepath: Path) -> None:
    """Execute multiple Cypher statements from a file."""
    print(f"📄 Running Cypher file: {filepath}")
    
    with open(filepath, 'r') as f:
        content = f.read()
    
    # Split by semicolon and filter out comments
    statements = [
        stmt.strip() 
        for stmt in content.split(';') 
        if stmt.strip() and not stmt.strip().startswith('//')
    ]
    
    for stmt in statements:
        if stmt:
            # Remove inline comments
            clean_stmt = '\n'.join(
                line.split('//')[0] 
                for line in stmt.split('\n')
            ).strip()
            
            if clean_stmt:
                try:
                    run_cypher(driver, clean_stmt)
                    print(f"  ✓ Executed: {clean_stmt[:50]}...")
                except Exception as e:
                    print(f"  ⚠ Warning executing statement: {e}")


def chunk(data: List[Any], size: int = 1000) -> Iterator[List[Any]]:
    """Split a list into chunks for batch processing."""
    for i in range(0, len(data), size):
        yield data[i:i + size]


def etl():
    """
    Main ETL function that migrates data from PostgreSQL to Neo4j.
    
    This function performs the complete Extract, Transform, Load process:
    1. Waits for both databases to be ready
    2. Sets up Neo4j schema using queries.cypher file
    3. Extracts data from PostgreSQL tables
    4. Transforms relational data into graph format
    5. Loads data into Neo4j with appropriate relationships
    
    The process creates the following graph structure:
    - Category nodes with name properties
    - Product nodes linked to categories via IN_CATEGORY relationships
    - Customer nodes with name and join_date properties
    - Order nodes linked to customers via PLACED relationships
    - Order-Product relationships via CONTAINS with quantity properties
    - Dynamic event relationships between customers and products
    """
    # Ensure dependencies are ready (useful when running in docker-compose)
    wait_for_postgres()
    wait_for_neo4j()

    # Get path to your Cypher schema file
    queries_path = Path(__file__).with_name("queries.cypher")

    # Connect to PostgreSQL
    print("\n🔌 Connecting to PostgreSQL...")
    pg_conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER", "app"),
        password=os.getenv("POSTGRES_PASSWORD", "apppass"),
        database=os.getenv("POSTGRES_DB", "shop"),
        sslmode="disable"
    )

    # Connect to Neo4j
    print("🔌 Connecting to Neo4j...")
    neo4j_uri = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
    neo4j_user = os.getenv("NEO4J_USER", "neo4j")
    neo4j_password = os.getenv("NEO4J_PASSWORD", "password")
    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

    try:
        # Apply Neo4j schema
        print("\n🏗️  Setting up Neo4j schema...")
        run_cypher_file(driver, queries_path)

        # Extract data from PostgreSQL
        print("\n📊 Extracting data from PostgreSQL...")
        
        df_categories = pd.read_sql("SELECT * FROM categories", pg_conn)
        df_products = pd.read_sql("SELECT * FROM products", pg_conn)
        df_customers = pd.read_sql("SELECT * FROM customers", pg_conn)
        df_orders = pd.read_sql("SELECT * FROM orders", pg_conn)
        df_order_items = pd.read_sql("SELECT * FROM order_items", pg_conn)
        df_events = pd.read_sql("SELECT * FROM events", pg_conn)
        
        print(f"  ✓ Categories: {len(df_categories)}")
        print(f"  ✓ Products: {len(df_products)}")
        print(f"  ✓ Customers: {len(df_customers)}")
        print(f"  ✓ Orders: {len(df_orders)}")
        print(f"  ✓ Order Items: {len(df_order_items)}")
        print(f"  ✓ Events: {len(df_events)}")

        # Load Categories
        print("\n📥 Loading Categories into Neo4j...")
        for _, row in df_categories.iterrows():
            run_cypher(driver, """
                MERGE (cat:Category {id: $id})
                SET cat.name = $name
            """, {"id": row["id"], "name": row["name"]})
        print(f"  ✓ Loaded {len(df_categories)} categories")

        # Load Products and link to Categories
        print("\n📥 Loading Products into Neo4j...")
        for _, row in df_products.iterrows():
            run_cypher(driver, """
                MERGE (p:Product {id: $id})
                SET p.name = $name, p.price = $price
                WITH p
                MATCH (cat:Category {id: $category_id})
                MERGE (p)-[:IN_CATEGORY]->(cat)
            """, {
                "id": row["id"],
                "name": row["name"],
                "price": float(row["price"]),
                "category_id": row["category_id"]
            })
        print(f"  ✓ Loaded {len(df_products)} products")

        # Load Customers
        print("\n📥 Loading Customers into Neo4j...")
        for _, row in df_customers.iterrows():
            run_cypher(driver, """
                MERGE (c:Customer {id: $id})
                SET c.name = $name, c.join_date = date($join_date)
            """, {
                "id": row["id"],
                "name": row["name"],
                "join_date": str(row["join_date"])
            })
        print(f"  ✓ Loaded {len(df_customers)} customers")

        # Load Orders and link to Customers
        print("\n📥 Loading Orders into Neo4j...")
        for _, row in df_orders.iterrows():
            run_cypher(driver, """
                MERGE (o:Order {id: $id})
                SET o.ts = datetime($ts)
                WITH o
                MATCH (c:Customer {id: $customer_id})
                MERGE (c)-[:PLACED]->(o)
            """, {
                "id": row["id"],
                "customer_id": row["customer_id"],
                "ts": row["ts"].isoformat()
            })
        print(f"  ✓ Loaded {len(df_orders)} orders")

        # Load Order Items (Order-Product relationships)
        print("\n📥 Loading Order Items into Neo4j...")
        for _, row in df_order_items.iterrows():
            run_cypher(driver, """
                MATCH (o:Order {id: $order_id})
                MATCH (p:Product {id: $product_id})
                MERGE (o)-[r:CONTAINS]->(p)
                SET r.quantity = $quantity
            """, {
                "order_id": row["order_id"],
                "product_id": row["product_id"],
                "quantity": int(row["quantity"])
            })
        print(f"  ✓ Loaded {len(df_order_items)} order items")

        # Load Events (Customer-Product behavioral relationships)
        print("\n📥 Loading Events into Neo4j...")
        event_type_map = {
            'view': 'VIEWED',
            'click': 'CLICKED',
            'add_to_cart': 'ADDED_TO_CART'
        }
        
        for _, row in df_events.iterrows():
            rel_type = event_type_map.get(row["event_type"], "INTERACTED")
            query = f"""
                MATCH (c:Customer {{id: $customer_id}})
                MATCH (p:Product {{id: $product_id}})
                CREATE (c)-[r:{rel_type}]->(p)
                SET r.ts = datetime($ts), r.event_id = $event_id
            """
            run_cypher(driver, query, {
                "customer_id": row["customer_id"],
                "product_id": row["product_id"],
                "ts": row["ts"].isoformat(),
                "event_id": row["id"]
            })
        print(f"  ✓ Loaded {len(df_events)} events")

        print("\n✅ ETL done.")

    finally:
        pg_conn.close()
        driver.close()


if __name__ == "__main__":
    etl()
