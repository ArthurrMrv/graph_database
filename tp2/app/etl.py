#!/usr/bin/env python3
"""
ETL script to migrate data from PostgreSQL to Neo4j.
Extracts relational data and transforms it into a graph structure.
"""

import os
import time
import sys
from pathlib import Path
from typing import List, Optional

import pandas as pd
import psycopg2
from neo4j import GraphDatabase
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection parameters
POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "user": os.getenv("POSTGRES_USER", "app"),
    "password": os.getenv("POSTGRES_PASSWORD", "password"),
    "database": os.getenv("POSTGRES_DB", "shop"),
}

NEO4J_CONFIG = {
    "uri": os.getenv("NEO4J_URI", "bolt://localhost:7687"),
    "user": os.getenv("NEO4J_USER", "neo4j"),
    "password": os.getenv("NEO4J_PASSWORD", "password"),
}

# Global Neo4j driver
neo4j_driver: Optional[GraphDatabase.driver] = None


def get_neo4j_driver():
    """Get or create Neo4j driver instance."""
    global neo4j_driver
    if neo4j_driver is None:
        neo4j_driver = GraphDatabase.driver(
            NEO4J_CONFIG["uri"],
            auth=(NEO4J_CONFIG["user"], NEO4J_CONFIG["password"]),
        )
    return neo4j_driver


def close_neo4j_driver():
    """Close Neo4j driver connection."""
    global neo4j_driver
    if neo4j_driver:
        neo4j_driver.close()
        neo4j_driver = None


def wait_for_postgres(max_retries: int = 30, delay: int = 2):
    """
    Wait for PostgreSQL to be ready to accept connections.
    
    Args:
        max_retries: Maximum number of connection attempts
        delay: Delay in seconds between attempts
    """
    print("Waiting for PostgreSQL to be ready...")
    for i in range(max_retries):
        try:
            conn = psycopg2.connect(**POSTGRES_CONFIG)
            conn.close()
            print("✓ PostgreSQL is ready")
            return
        except psycopg2.OperationalError as e:
            if i < max_retries - 1:
                print(f"  Attempt {i+1}/{max_retries}: PostgreSQL not ready yet, retrying in {delay}s...")
                time.sleep(delay)
            else:
                print(f"✗ Failed to connect to PostgreSQL after {max_retries} attempts")
                raise


def wait_for_neo4j(max_retries: int = 30, delay: int = 2):
    """
    Wait for Neo4j to be ready to accept connections.
    
    Args:
        max_retries: Maximum number of connection attempts
        delay: Delay in seconds between attempts
    """
    print("Waiting for Neo4j to be ready...")
    for i in range(max_retries):
        try:
            driver = get_neo4j_driver()
            with driver.session() as session:
                session.run("RETURN 1")
            print("✓ Neo4j is ready")
            return
        except Exception as e:
            if i < max_retries - 1:
                print(f"  Attempt {i+1}/{max_retries}: Neo4j not ready yet, retrying in {delay}s...")
                time.sleep(delay)
            else:
                print(f"✗ Failed to connect to Neo4j after {max_retries} attempts: {e}")
                raise


def run_cypher(query: str, parameters: dict = None):
    """
    Execute a single Cypher query.
    
    Args:
        query: Cypher query string
        parameters: Optional parameters dictionary
    """
    driver = get_neo4j_driver()
    with driver.session() as session:
        result = session.run(query, parameters or {})
        return list(result)


def run_cypher_file(file_path: Path):
    """
    Execute multiple Cypher statements from a file.
    Splits by semicolon and executes each statement.
    
    Args:
        file_path: Path to the Cypher file
    """
    if not file_path.exists():
        raise FileNotFoundError(f"Cypher file not found: {file_path}")
    
    with open(file_path, "r") as f:
        content = f.read()
    
    # Split by semicolon and filter out empty statements
    statements = [s.strip() for s in content.split(";") if s.strip()]
    
    driver = get_neo4j_driver()
    with driver.session() as session:
        for statement in statements:
            if statement:
                try:
                    session.run(statement)
                except Exception as e:
                    # Some statements might fail if constraints/indexes already exist
                    if "already exists" not in str(e).lower():
                        print(f"Warning: {e}")
    
    print(f"✓ Executed {len(statements)} statements from {file_path.name}")


def chunk(dataframe: pd.DataFrame, chunk_size: int = 1000) -> List[pd.DataFrame]:
    """
    Split a DataFrame into chunks for batch processing.
    
    Args:
        dataframe: DataFrame to chunk
        chunk_size: Size of each chunk
    
    Returns:
        List of DataFrame chunks
    """
    return [dataframe[i : i + chunk_size] for i in range(0, len(dataframe), chunk_size)]


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
    
    print("\n=== Setting up Neo4j schema ===")
    run_cypher_file(queries_path)
    
    print("\n=== Extracting data from PostgreSQL ===")
    # Connect to PostgreSQL
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    
    # Extract all tables
    categories_df = pd.read_sql("SELECT * FROM categories", conn)
    products_df = pd.read_sql("SELECT * FROM products", conn)
    customers_df = pd.read_sql("SELECT * FROM customers", conn)
    orders_df = pd.read_sql("SELECT * FROM orders", conn)
    order_items_df = pd.read_sql("SELECT * FROM order_items", conn)
    events_df = pd.read_sql("SELECT * FROM events", conn)
    
    conn.close()
    
    print(f"  Extracted {len(categories_df)} categories")
    print(f"  Extracted {len(products_df)} products")
    print(f"  Extracted {len(customers_df)} customers")
    print(f"  Extracted {len(orders_df)} orders")
    print(f"  Extracted {len(order_items_df)} order items")
    print(f"  Extracted {len(events_df)} events")
    
    print("\n=== Loading data into Neo4j ===")
    driver = get_neo4j_driver()
    
    with driver.session() as session:
        # 1. Create Category nodes
        print("  Creating Category nodes...")
        for _, row in categories_df.iterrows():
            session.run(
                """
                MERGE (c:Category {id: $id})
                SET c.name = $name
                """,
                {"id": row["id"], "name": row["name"]},
            )
        print(f"    ✓ Created {len(categories_df)} categories")
        
        # 2. Create Product nodes and link to categories
        print("  Creating Product nodes...")
        for _, row in products_df.iterrows():
            session.run(
                """
                MERGE (p:Product {id: $id})
                SET p.name = $name, p.price = $price
                WITH p
                MATCH (cat:Category {id: $category_id})
                MERGE (p)-[:IN_CATEGORY]->(cat)
                """,
                {
                    "id": row["id"],
                    "name": row["name"],
                    "price": float(row["price"]),
                    "category_id": row["category_id"],
                },
            )
        print(f"    ✓ Created {len(products_df)} products")
        
        # 3. Create Customer nodes
        print("  Creating Customer nodes...")
        for _, row in customers_df.iterrows():
            session.run(
                """
                MERGE (c:Customer {id: $id})
                SET c.name = $name, c.join_date = $join_date
                """,
                {
                    "id": row["id"],
                    "name": row["name"],
                    "join_date": str(row["join_date"]),
                },
            )
        print(f"    ✓ Created {len(customers_df)} customers")
        
        # 4. Create Order nodes and link to customers
        print("  Creating Order nodes...")
        for _, row in orders_df.iterrows():
            session.run(
                """
                MATCH (c:Customer {id: $customer_id})
                MERGE (o:Order {id: $id})
                SET o.ts = $ts
                MERGE (c)-[:PLACED]->(o)
                """,
                {
                    "id": row["id"],
                    "customer_id": row["customer_id"],
                    "ts": str(row["ts"]),
                },
            )
        print(f"    ✓ Created {len(orders_df)} orders")
        
        # 5. Create Order-Product relationships
        print("  Creating Order-Product relationships...")
        for _, row in order_items_df.iterrows():
            session.run(
                """
                MATCH (o:Order {id: $order_id})
                MATCH (p:Product {id: $product_id})
                MERGE (o)-[r:CONTAINS]->(p)
                SET r.quantity = $quantity
                """,
                {
                    "order_id": row["order_id"],
                    "product_id": row["product_id"],
                    "quantity": int(row["quantity"]),
                },
            )
        print(f"    ✓ Created {len(order_items_df)} order-item relationships")
        
        # 6. Create event relationships (VIEWED, CLICKED, ADDED_TO_CART)
        print("  Creating event relationships...")
        event_type_map = {
            "view": "VIEWED",
            "click": "CLICKED",
            "add_to_cart": "ADDED_TO_CART",
        }
        
        for _, row in events_df.iterrows():
            rel_type = event_type_map.get(row["event_type"], "VIEWED")
            session.run(
                f"""
                MATCH (c:Customer {{id: $customer_id}})
                MATCH (p:Product {{id: $product_id}})
                MERGE (c)-[r:{rel_type}]->(p)
                SET r.ts = $ts, r.event_id = $event_id
                """,
                {
                    "customer_id": row["customer_id"],
                    "product_id": row["product_id"],
                    "ts": str(row["ts"]),
                    "event_id": row["id"],
                },
            )
        print(f"    ✓ Created {len(events_df)} event relationships")
    
    print("\n=== ETL completed successfully ===")
    print("ETL done.")


if __name__ == "__main__":
    try:
        etl()
    except KeyboardInterrupt:
        print("\nETL interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ ETL failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        close_neo4j_driver()

