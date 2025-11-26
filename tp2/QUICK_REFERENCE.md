# E-Commerce Graph Recommendation System - Quick Reference

## 🎯 System Status

✅ **All Services Running**
- PostgreSQL: Port 5432
- Neo4j: Ports 7474 (HTTP), 7687 (Bolt)
- FastAPI: Port 8000

## 📊 Current Data

**Nodes:** 12 total
- Customers: 3
- Products: 4
- Orders: 3
- Categories: 2

**Relationships:** 17 total
- PLACED: 3
- CONTAINS: 5
- IN_CATEGORY: 4
- VIEWED: 3
- CLICKED: 1
- ADDED_TO_CART: 1

## 🔗 Quick Links

- **Neo4j Browser**: http://localhost:7474
  - Username: `neo4j`
  - Password: `password`

- **FastAPI Docs**: http://localhost:8000/docs

- **API Health**: http://localhost:8000/health

- **API Stats**: http://localhost:8000/stats

## 🔍 Sample Neo4j Queries

### View All Constraints
```cypher
SHOW CONSTRAINTS;
```

### Count All Nodes
```cypher
MATCH (n) RETURN count(n) AS total_nodes;
```

### View Customer Orders
```cypher
MATCH (c:Customer)-[:PLACED]->(o:Order)-[:CONTAINS]->(p:Product)
RETURN c.name AS customer, o.id AS order_id, collect(p.name) AS products;
```

### Product Co-occurrence (Bought Together)
```cypher
MATCH (p1:Product)<-[:CONTAINS]-(o:Order)-[:CONTAINS]->(p2:Product)
WHERE p1 <> p2
RETURN p1.name, p2.name, count(o) AS times_together
ORDER BY times_together DESC;
```

### Customer Interactions
```cypher
MATCH (c:Customer)-[r]->(p:Product)
WHERE type(r) IN ['VIEWED', 'CLICKED', 'ADDED_TO_CART']
RETURN c.name, type(r) AS action, p.name, r.ts
ORDER BY r.ts;
```

### Products by Category
```cypher
MATCH (p:Product)-[:IN_CATEGORY]->(cat:Category)
RETURN cat.name AS category, collect(p.name) AS products;
```

### Visualize Entire Graph
```cypher
MATCH (n) RETURN n LIMIT 50;
```

### Customer Purchase History
```cypher
MATCH (c:Customer {name: 'Alice'})-[:PLACED]->(o:Order)-[:CONTAINS]->(p:Product)
RETURN c.name, o.ts, p.name, p.price;
```

### Find Similar Customers (by products viewed)
```cypher
MATCH (c1:Customer)-[:VIEWED]->(p:Product)<-[:VIEWED]-(c2:Customer)
WHERE c1 <> c2
RETURN c1.name, c2.name, collect(p.name) AS common_products;
```

## 🐳 Docker Commands

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
docker compose logs -f app
```

### Run ETL Manually
```bash
docker compose exec app python etl.py
```

### Run Tests
```bash
bash scripts/check_containers.sh
```

### PostgreSQL Shell
```bash
docker compose exec postgres psql -U app -d shop
```

## 📝 PostgreSQL Sample Queries

```sql
-- View all customers
SELECT * FROM customers;

-- Customer orders with products
SELECT c.name, o.id, p.name, oi.quantity, p.price
FROM customers c
JOIN orders o ON c.id = o.customer_id
JOIN order_items oi ON o.id = oi.order_id
JOIN products p ON oi.product_id = p.id;

-- Customer behavior
SELECT c.name, e.event_type, p.name, e.ts
FROM customers c
JOIN events e ON c.id = e.customer_id
JOIN products p ON e.product_id = p.id
ORDER BY e.ts;
```

## 🔧 Troubleshooting

### Reset Everything
```bash
docker compose down -v
rm -rf neo4j/data/*
docker compose up -d
```

### Check Container Status
```bash
docker compose ps
```

### View All Logs
```bash
docker compose logs
```

## 📚 Files Reference

- `docker-compose.yml` - Service orchestration
- `app/etl.py` - ETL pipeline
- `app/main.py` - FastAPI application
- `app/queries.cypher` - Neo4j schema
- `postgres/init/01_schema.sql` - Database schema
- `postgres/init/02_seed.sql` - Sample data
- `scripts/check_containers.sh` - Automated tests
- `Instructions.md` - Comprehensive guide

## ✅ Verification Checklist

- [x] PostgreSQL initialized with schema and data
- [x] Neo4j running with APOC and GDS plugins
- [x] ETL completed successfully
- [x] API health check passing
- [x] API stats endpoint working
- [x] Automated tests passing
- [x] Neo4j Browser accessible
- [x] All 12 nodes loaded
- [x] All 17 relationships created

---

**Status**: 🟢 System fully operational and ready for use!
