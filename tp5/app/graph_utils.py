from neo4j import GraphDatabase

class GraphUtils:
    def __init__(self, driver):
        self.driver = driver

    def explore_graph_schema(self):
        with self.driver.session() as session:
            print("\n--- Graph Schema ---")
            labels = session.run("CALL db.labels() YIELD label RETURN collect(label) as labels").single()["labels"]
            print(f"Node Labels: {labels}")
            
            rels = session.run("CALL db.relationshipTypes() YIELD relationshipType RETURN collect(relationshipType) as types").single()["types"]
            print(f"Relationship Types: {rels}")
            
            counts = session.run("MATCH (n) RETURN count(n) as nodes").single()["nodes"]
            rel_counts = session.run("MATCH ()-[r]->() RETURN count(r) as rels").single()["rels"]
            print(f"Total Nodes: {counts}")
            print(f"Total Relationships: {rel_counts}")
            print("--------------------\n")

    def query_person_relationships(self, person_name):
        with self.driver.session() as session:
            print(f"\n--- Relationships for {person_name} ---")
            result = session.run("""
                MATCH (p:Person {name: $name})-[r]-(related)
                RETURN type(r) as relationship, related.name as entity, labels(related) as type
                LIMIT 10
            """, name=person_name)
            
            records = list(result)
            if not records:
                print("No relationships found.")
            else:
                for record in records:
                    print(f"- [{record['relationship']}] -> {record['entity']} ({record['type']})")
            print("--------------------------------------\n")
