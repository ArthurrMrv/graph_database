from database import db

def load_data():
    driver = db.get_driver()
    with driver.session() as session:
        # 1. Create Constraints
        print("Creating constraints...")
        session.run("CREATE CONSTRAINT user_id IF NOT EXISTS FOR (u:User) REQUIRE u.id IS UNIQUE")
        session.run("CREATE CONSTRAINT tweet_id IF NOT EXISTS FOR (t:Tweet) REQUIRE t.id IS UNIQUE")

        # 2. Load Users
        print("Loading Users...")
        session.run("""
            LOAD CSV WITH HEADERS FROM 'https://bit.ly/39JYakC' AS row
            MERGE (u:User {id: row.id})
            SET u.username = row.username,
                u.name = row.name,
                u.registeredAt = row.registeredAt
        """)

        # 3. Load Tweets
        print("Loading Tweets...")
        session.run("""
            LOAD CSV WITH HEADERS FROM 'https://bit.ly/3y3ODyc' AS row
            MERGE (t:Tweet {id: row.id})
            SET t.text = row.text,
                t.createdAt = row.createdAt
        """)
        
        # 4. Load PUBLISH relationships (User -> Tweet)
        # The tweets CSV contains authorId
        print("Loading PUBLISH relationships...")
        session.run("""
            LOAD CSV WITH HEADERS FROM 'https://bit.ly/3y3ODyc' AS row
            MATCH (u:User {id: row.authorId})
            MATCH (t:Tweet {id: row.id})
            MERGE (u)-[:PUBLISH]->(t)
        """)

        # 5. Load Followers
        print("Loading Followers...")
        session.run("""
            LOAD CSV WITH HEADERS FROM 'https://bit.ly/3n08lEL' AS row
            MATCH (u1:User {id: row.source})
            MATCH (u2:User {id: row.target})
            MERGE (u1)-[:FOLLOWS]->(u2)
        """)

        # 6. Load Mentions
        print("Loading Mentions...")
        session.run("""
            LOAD CSV WITH HEADERS FROM 'https://bit.ly/3tINZ6D' AS row
            MATCH (t:Tweet {id: row.tweetId})
            MATCH (u:User {id: row.userId})
            MERGE (t)-[:MENTIONS]->(u)
        """)

        # 7. Load Retweets
        print("Loading Retweets...")
        session.run("""
            LOAD CSV WITH HEADERS FROM 'https://bit.ly/3QyDrRl' AS row
            MATCH (t1:Tweet {id: row.source})
            MATCH (t2:Tweet {id: row.target})
            MERGE (t1)-[:RETWEETS]->(t2)
        """)

        # 8. Load Replies
        print("Loading Replies...")
        session.run("""
            LOAD CSV WITH HEADERS FROM 'https://bit.ly/3b9Wgdx' AS row
            MATCH (t1:Tweet {id: row.source})
            MATCH (t2:Tweet {id: row.target})
            MERGE (t1)-[:IN_REPLY_TO]->(t2)
        """)

        print("Data loading complete.")
        return {"status": "success", "message": "Data loaded successfully"}
