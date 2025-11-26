# E-Commerce Graph Recommendations - Docker Instructions

## 📋 Prerequisites

Before starting, ensure you have:

- **Docker Desktop** installed and running
- **4 GB free RAM** for Neo4j and GDS plugin
- **Available ports**:
  - `5432` (PostgreSQL)
  - `7474` & `7687` (Neo4j)
  - `8000` (FastAPI)

## 🚀 Quick Start

### 1. Start the Stack

From the `tp2` directory, run:

```bash
docker compose up -d
```

This will start three services:
- **postgres**: PostgreSQL database with e-commerce data
- **neo4j**: Neo4j graph database with APOC and GDS plugins
- **app**: FastAPI application with ETL capabilities

### 2. Check Logs

Monitor the startup process:

```bash
docker compose logs -f
```

Wait until you see:
- Postgres: `database system is ready to accept connections`
- Neo4j: `Remote interface available at http://localhost:7474/`
- App: `Uvicorn running on http://0.0.0.0:8000`

Press `Ctrl+C` to exit log viewing.

### 3. Verify Services

Check that all containers are running:

```bash
docker compose ps
```

You should see three containers with status "Up".

## 🔄 Running the ETL

The ETL (Extract, Transform, Load) process migrates data from PostgreSQL to Neo4j.

### Automatic ETL (First Startup)

The ETL runs automatically when the app container starts for the first time.

### Manual ETL

To run the ETL manually:

```bash
docker compose exec app python etl.py
```

Expected output:
```
⏳ Waiting for PostgreSQL...
✅ PostgreSQL is ready!
⏳ Waiting for Neo4j...
✅ Neo4j is ready!

🔌 Connecting to PostgreSQL...
🔌 Connecting to Neo4j...

🏗️  Setting up Neo4j schema...
📄 Running Cypher file: /work/app/queries.cypher
  ✓ Executed: CREATE CONSTRAINT customer_id IF NOT EXISTS...
  ...

📊 Extracting data from PostgreSQL...
  ✓ Categories: 2
  ✓ Products: 4
  ✓ Customers: 3
  ✓ Orders: 3
  ✓ Order Items: 5
  ✓ Events: 5

📥 Loading Categories into Neo4j...
  ✓ Loaded 2 categories
📥 Loading Products into Neo4j...
  ✓ Loaded 4 products
📥 Loading Customers into Neo4j...
  ✓ Loaded 3 customers
📥 Loading Orders into Neo4j...
  ✓ Loaded 3 orders
📥 Loading Order Items into Neo4j...
  ✓ Loaded 5 order items
📥 Loading Events into Neo4j...
  ✓ Loaded 5 events

✅ ETL done.
```

## 🔍 Exploring the Data

### PostgreSQL

#### Check Database Schema

```bash
docker compose exec postgres psql -U app -d shop -c "\dt"
```

Expected output:
```
          List of relations
 Schema |    Name     | Type  | Owner 
--------+-------------+-------+-------
 public | categories  | table | app
 public | customers   | table | app
 public | events      | table | app
 public | order_items | table | app
 public | orders      | table | app
 public | products    | table | app
(6 rows)
```

#### Query Data

```bash
# Count customers
docker compose exec postgres psql -U app -d shop -c "SELECT count(*) FROM customers;"

# View all orders
docker compose exec postgres psql -U app -d shop -c "SELECT * FROM orders;"

# View events
docker compose exec postgres psql -U app -d shop -c "SELECT * FROM events;"
```

### Neo4j Browser

1. Open your browser and navigate to: **http://localhost:7474**

2. Login with:
   - **Username**: `neo4j`
   - **Password**: `password`

3. Try these Cypher queries:

```cypher
// Count all nodes
MATCH (n) RETURN count(n) AS total_nodes;

// Count customers
MATCH (c:Customer) RETURN count(c) AS customers;

// Count products
MATCH (p:Product) RETURN count(p) AS products;

// View customer orders
MATCH (c:Customer)-[:PLACED]->(o:Order)
RETURN c.name, o.id, o.ts;

// View products by category
MATCH (p:Product)-[:IN_CATEGORY]->(cat:Category)
RETURN cat.name AS category, collect(p.name) AS products;

// View customer interactions
MATCH (c:Customer)-[r]->(p:Product)
WHERE type(r) IN ['VIEWED', 'CLICKED', 'ADDED_TO_CART']
RETURN c.name, type(r) AS interaction, p.name, r.ts;

// Visualize the entire graph
MATCH (n) RETURN n LIMIT 50;
```

4. View constraints:

```cypher
SHOW CONSTRAINTS;
```

5. View indexes:

```cypher
SHOW INDEXES;
```

## 🌐 Testing the API

### Health Check

```bash
curl http://localhost:8000/health
```

Expected response:
```json
{"ok":true}
```

### API Documentation

Open your browser and navigate to: **http://localhost:8000/docs**

This will show the interactive Swagger UI documentation.

### Get Statistics

```bash
curl http://localhost:8000/stats
```

This returns counts of all nodes and relationships in the graph.

## 🧪 Running Automated Tests

### Local Testing (from host machine)

Make the test script executable:

```bash
chmod +x scripts/check_containers.sh
```

Run the tests:

```bash
bash scripts/check_containers.sh
```

### Container Testing (inside Docker)

Run the checks service:

```bash
docker compose run --rm checks
```

This will:
1. Check FastAPI health endpoint
2. Validate PostgreSQL connectivity and data
3. Run the ETL and verify output

Expected output:
```
🧪 Running E-Commerce Graph System Tests

› FastAPI health check
✔ FastAPI health OK
{"ok":true}

› Postgres: SELECT * FROM orders LIMIT 5;
 id | customer_id |           ts           
----+-------------+------------------------
 O1 | C1          | 2024-04-01 10:15:00+00
 O2 | C2          | 2024-04-02 12:30:00+00
 O3 | C1          | 2024-04-05 08:05:00+00
(3 rows)

✔ Orders query OK

› Postgres: SELECT now();
              now              
-------------------------------
 2025-11-19 09:03:32.877638+00
(1 row)

✔ now() query OK

› ETL: python /work/app/etl.py
...
ETL done.
✔ ETL output OK (ETL done.)

✅ All tests passed!
```

## 🛠️ Common Commands

### Start Services

```bash
docker compose up -d
```

### Stop Services

```bash
docker compose down
```

### View Logs

```bash
# All services
docker compose logs -f

# Specific service
docker compose logs -f app
docker compose logs -f neo4j
docker compose logs -f postgres
```

### Restart Services

```bash
# All services
docker compose restart

# Specific service
docker compose restart app
```

### Execute Commands in Containers

```bash
# Access app container shell
docker compose exec app bash

# Access postgres shell
docker compose exec postgres psql -U app -d shop

# Run Python script
docker compose exec app python etl.py
```

## 🔧 Troubleshooting

### API Returns `{ok:false}` at `/health`

1. Check if containers are running:
   ```bash
   docker compose ps
   ```

2. Check logs:
   ```bash
   docker compose logs -f app neo4j postgres
   ```

3. Verify Neo4j is accessible:
   - Open http://localhost:7474
   - Run: `RETURN 1;`

### Connection Refused on ETL

The ETL includes wait logic for both databases. If it still fails:

1. Give services more time to start
2. Restart services:
   ```bash
   docker compose restart
   ```

### Neo4j Out of Memory

If Neo4j runs out of memory during operations, the docker-compose.yml already includes:
```yaml
NEO4J_server_memory_heap_max__size: 2G
NEO4J_server_memory_heap_initial__size: 1G
```

If you need more, edit `docker-compose.yml` and increase these values.

### APOC/GDS Procedures Not Available

The docker-compose.yml enables APOC and GDS plugins. If you still have issues:

1. Check Neo4j logs:
   ```bash
   docker compose logs neo4j | grep -i plugin
   ```

2. Verify in Neo4j Browser:
   ```cypher
   CALL dbms.procedures() YIELD name
   WHERE name STARTS WITH 'apoc' OR name STARTS WITH 'gds'
   RETURN name LIMIT 10;
   ```

### Duplicate Event Edges After Re-running ETL

The ETL creates new relationships each time. To reset event relationships:

```cypher
MATCH (:Customer)-[r:VIEWED|CLICKED|ADDED_TO_CART]->(:Product)
DELETE r;
```

Then re-run the ETL.

### Full Reset (Warning: Deletes All Data)

```bash
# Stop and remove containers, networks, and volumes
docker compose down -v

# Remove Neo4j data directory
rm -rf neo4j/data

# Start fresh
docker compose up -d
```

The ETL will run automatically on first startup.

## 📊 Data Overview

### PostgreSQL Tables

- **customers**: 3 customers (Alice, Bob, Chloé)
- **categories**: 2 categories (Electronics, Books)
- **products**: 4 products (Mouse, Hub, Book, Keyboard)
- **orders**: 3 orders
- **order_items**: 5 order line items
- **events**: 5 behavioral events (view, click, add_to_cart)

### Neo4j Graph Structure

**Nodes:**
- `Customer` (3 nodes)
- `Product` (4 nodes)
- `Category` (2 nodes)
- `Order` (3 nodes)

**Relationships:**
- `(:Customer)-[:PLACED]->(:Order)` - Customer placed an order
- `(:Order)-[:CONTAINS]->(:Product)` - Order contains products
- `(:Product)-[:IN_CATEGORY]->(:Category)` - Product belongs to category
- `(:Customer)-[:VIEWED]->(:Product)` - Customer viewed product
- `(:Customer)-[:CLICKED]->(:Product)` - Customer clicked product
- `(:Customer)-[:ADDED_TO_CART]->(:Product)` - Customer added to cart

## 🎯 Next Steps

1. **Explore the Graph**: Use Neo4j Browser to visualize relationships
2. **Add Recommendation Endpoints**: Extend `app/main.py` with recommendation algorithms
3. **Implement Graph Algorithms**: Use GDS for PageRank, similarity, etc.
4. **Add More Data**: Extend the seed data in `postgres/init/02_seed.sql`

## 📝 Notes

- The FastAPI server supports hot reload - edit `app/main.py` and changes will reload automatically
- Neo4j data persists in `neo4j/data/` directory
- PostgreSQL data persists in a Docker volume
- All logs are available via `docker compose logs`

---

Happy coding! 🚀
