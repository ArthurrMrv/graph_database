from neo4j import GraphDatabase
import os

class Loader:
    def __init__(self, driver):
        self.driver = driver

    def load_data(self):
        with self.driver.session() as session:
            # 1. Create Constraints
            print("Creating constraints...")
            session.run("CREATE CONSTRAINT stream_id IF NOT EXISTS FOR (s:Stream) REQUIRE s.id IS UNIQUE")
            session.run("CREATE CONSTRAINT user_id IF NOT EXISTS FOR (u:User) REQUIRE u.id IS UNIQUE")

            # 2. Generate Synthetic Data
            print("Generating Synthetic Data...")
            
            # Parameters
            NUM_STREAMERS = 200
            NUM_USERS = 2000
            LANGUAGES = ['EN', 'FR', 'ES', 'DE', 'RU']
            
            import random
            
            # Generate Streamers
            print(f"Creating {NUM_STREAMERS} streamers...")
            streamers = []
            for i in range(NUM_STREAMERS):
                lang = random.choice(LANGUAGES)
                stream_id = str(i)
                name = f"Streamer_{i}_{lang}"
                streamers.append({'id': stream_id, 'name': name, 'lang': lang})
            
            # Batch load streamers
            session.run("""
                UNWIND $batch AS row
                MERGE (s:Stream {id: row.id})
                SET s.language = row.lang,
                    s.name = row.name
            """, batch=streamers)

            # Generate Audience (Users and Watches)
            print(f"Creating {NUM_USERS} users and relationships...")
            relationships = []
            
            for i in range(NUM_USERS):
                user_id = str(i)
                # Assign a preferred language to the user to create community structure
                pref_lang = random.choice(LANGUAGES)
                
                # User watches 5-15 streams
                num_watches = random.randint(5, 15)
                
                for _ in range(num_watches):
                    # 80% chance to watch stream of preferred language, 20% random
                    if random.random() < 0.8:
                        # Pick from streamers of preferred language
                        candidates = [s for s in streamers if s['lang'] == pref_lang]
                    else:
                        # Pick from all streamers
                        candidates = streamers
                    
                    if candidates:
                        target_stream = random.choice(candidates)
                        relationships.append({'userId': user_id, 'streamId': target_stream['id']})

            # Batch load users and relationships
            # Loading in chunks to avoid memory issues if large
            BATCH_SIZE = 1000
            for i in range(0, len(relationships), BATCH_SIZE):
                batch = relationships[i:i + BATCH_SIZE]
                session.run("""
                    UNWIND $batch AS row
                    MERGE (u:User {id: row.userId})
                    MATCH (s:Stream {id: row.streamId})
                    MERGE (u)-[:WATCHES]->(s)
                """, batch=batch)

            # 4. Create Monopartite Graph (Stream-Stream shared audience)
            print("Creating Monopartite Graph (Shared Audience)...")
            session.run("""
                MATCH (u:User)-[:WATCHES]->(s1:Stream)
                MATCH (u)-[:WATCHES]->(s2:Stream)
                WHERE id(s1) < id(s2)
                WITH s1, s2, count(u) as shared_audience
                MERGE (s1)-[r:SHARED_AUDIENCE]->(s2)
                SET r.weight = shared_audience
            """)
            
            print("Data loading complete.")
