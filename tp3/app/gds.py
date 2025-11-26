from database import db

def create_projections():
    driver = db.get_driver()
    with driver.session() as session:
        # Check if projection exists
        exists = session.run("CALL gds.graph.exists('twitter-graph') YIELD exists RETURN exists").single()["exists"]
        if not exists:
            print("Creating directed projection 'twitter-graph'...")
            session.run("""
                CALL gds.graph.project(
                    'twitter-graph',
                    'User',
                    'FOLLOWS'
                )
            """)
        
        # Undirected projection for some algos like Triangle Count or Community Detection if needed
        exists_undirected = session.run("CALL gds.graph.exists('twitter-graph-undirected') YIELD exists RETURN exists").single()["exists"]
        if not exists_undirected:
            print("Creating undirected projection 'twitter-graph-undirected'...")
            session.run("""
                CALL gds.graph.project(
                    'twitter-graph-undirected',
                    'User',
                    {
                        FOLLOWS: {
                            orientation: 'UNDIRECTED'
                        }
                    }
                )
            """)
            
    return {"status": "success", "message": "Projections created"}

def run_pagerank():
    driver = db.get_driver()
    with driver.session() as session:
        result = session.run("""
            CALL gds.pageRank.stream('twitter-graph')
            YIELD nodeId, score
            RETURN gds.util.asNode(nodeId).username AS username, score
            ORDER BY score DESC
            LIMIT 10
        """)
        return [record.data() for record in result]

def run_betweenness():
    driver = db.get_driver()
    with driver.session() as session:
        result = session.run("""
            CALL gds.betweenness.stream('twitter-graph')
            YIELD nodeId, score
            RETURN gds.util.asNode(nodeId).username AS username, score
            ORDER BY score DESC
            LIMIT 10
        """)
        return [record.data() for record in result]

def run_degree_centrality():
    driver = db.get_driver()
    with driver.session() as session:
        # In-degree (Followers)
        result = session.run("""
            CALL gds.degree.stream('twitter-graph', { orientation: 'REVERSE' })
            YIELD nodeId, score
            RETURN gds.util.asNode(nodeId).username AS username, score AS followers
            ORDER BY score DESC
            LIMIT 10
        """)
        return [record.data() for record in result]

def run_louvain():
    driver = db.get_driver()
    with driver.session() as session:
        result = session.run("""
            CALL gds.louvain.stream('twitter-graph')
            YIELD nodeId, communityId
            RETURN gds.util.asNode(nodeId).username AS username, communityId
            LIMIT 20
        """)
        return [record.data() for record in result]

def run_triangle_count():
    driver = db.get_driver()
    with driver.session() as session:
        result = session.run("""
            CALL gds.triangleCount.stream('twitter-graph-undirected')
            YIELD nodeId, triangleCount
            RETURN gds.util.asNode(nodeId).username AS username, triangleCount
            ORDER BY triangleCount DESC
            LIMIT 10
        """)
        return [record.data() for record in result]

def run_dijkstra(start_user, end_user):
    driver = db.get_driver()
    with driver.session() as session:
        # Find node IDs first
        start_node = session.run("MATCH (u:User {username: $username}) RETURN id(u) as id", username=start_user).single()
        end_node = session.run("MATCH (u:User {username: $username}) RETURN id(u) as id", username=end_user).single()
        
        if not start_node or not end_node:
            return {"error": "User not found"}
            
        result = session.run("""
            CALL gds.shortestPath.dijkstra.stream('twitter-graph', {
                sourceNode: $startNode,
                targetNode: $endNode
            })
            YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path
            RETURN
                index,
                gds.util.asNode(sourceNode).username AS source,
                gds.util.asNode(targetNode).username AS target,
                totalCost,
                [nodeId IN nodeIds | gds.util.asNode(nodeId).username] AS nodeNames,
                costs
        """, startNode=start_node["id"], endNode=end_node["id"])
        
        return [record.data() for record in result]
