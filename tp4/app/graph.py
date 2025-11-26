from neo4j import GraphDatabase

class GraphManager:
    def __init__(self, driver):
        self.driver = driver

    def create_projection(self):
        with self.driver.session() as session:
            print("Creating Graph Projection...")
            exists = session.run("CALL gds.graph.exists('twitch-graph') YIELD exists RETURN exists").single()["exists"]
            if exists:
                session.run("CALL gds.graph.drop('twitch-graph')")
            
            # Project Stream nodes and SHARED_AUDIENCE relationships
            # Undirected because shared audience is symmetric
            session.run("""
                CALL gds.graph.project(
                    'twitch-graph',
                    'Stream',
                    {
                        SHARED_AUDIENCE: {
                            orientation: 'UNDIRECTED',
                            properties: 'weight'
                        }
                    }
                )
            """)
            print("Graph Projection created.")

    def run_node2vec(self):
        with self.driver.session() as session:
            print("Running Node2Vec...")
            # Parameters from README:
            # embeddingDimension: 8
            # relationshipWeightProperty: 'weight'
            # inOutFactor: 0.5
            # returnFactor: 1
            
            session.run("""
                CALL gds.node2vec.write('twitch-graph', {
                    embeddingDimension: 8,
                    relationshipWeightProperty: 'weight',
                    inOutFactor: 0.5,
                    returnFactor: 1.0,
                    writeProperty: 'embedding'
                })
            """)
            print("Node2Vec embeddings generated and written to 'embedding' property.")
