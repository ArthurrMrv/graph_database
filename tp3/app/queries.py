from database import db

def get_question_answer(question_id: int):
    driver = db.get_driver()
    query = ""
    
    if question_id == 1:
        # 1. Find five random user nodes
        query = "MATCH (u:User) RETURN u LIMIT 5"
    elif question_id == 2:
        # 2. Find five random FOLLOWS relationships
        query = "MATCH (u1:User)-[r:FOLLOWS]->(u2:User) RETURN u1.username, type(r), u2.username LIMIT 5"
    elif question_id == 3:
        # 3. Find the text property of three random Tweet nodes
        query = "MATCH (t:Tweet) RETURN t.text LIMIT 3"
    elif question_id == 4:
        # 4. Visualize sample RETWEETS relationships
        query = "MATCH (t1:Tweet)-[r:RETWEETS]->(t2:Tweet) RETURN t1.id, type(r), t2.id LIMIT 5"
    elif question_id == 5:
        # 5. Calculate missing values ratio for createdAt (assuming we check all tweets)
        query = """
        MATCH (t:Tweet)
        WITH count(t) as total, count(t.createdAt) as present
        RETURN 1.0 - (toFloat(present) / total) as missing_ratio
        """
    elif question_id == 6:
        # 6. Count relationships by type
        query = "MATCH ()-[r]->() RETURN type(r) as type, count(r) as count"
    elif question_id == 7:
        # 7. Compare retweet and original tweet text (sample)
        query = """
        MATCH (t1:Tweet)-[:RETWEETS]->(t2:Tweet)
        RETURN t1.text as retweet_text, t2.text as original_text LIMIT 5
        """
    elif question_id == 8:
        # 8. Calculate tweet distribution by year
        query = """
        MATCH (t:Tweet)
        WHERE t.createdAt IS NOT NULL
        RETURN substring(t.createdAt, 0, 4) as year, count(t) as count
        ORDER BY year
        """
    elif question_id == 9:
        # 9. Find tweets created in 2021
        query = """
        MATCH (t:Tweet)
        WHERE t.createdAt STARTS WITH '2021'
        RETURN t.id, t.text, t.createdAt LIMIT 10
        """
    elif question_id == 10:
        # 10. Return top days with highest tweet count
        query = """
        MATCH (t:Tweet)
        WHERE t.createdAt IS NOT NULL
        RETURN substring(t.createdAt, 0, 10) as day, count(t) as count
        ORDER BY count DESC LIMIT 5
        """
    elif question_id == 11:
        # 11. Count users mentioned without tweets (Users who are mentioned but haven't published any tweets)
        query = """
        MATCH (t:Tweet)-[:MENTIONS]->(u:User)
        WHERE NOT (u)-[:PUBLISH]->(:Tweet)
        RETURN count(DISTINCT u) as users_mentioned_without_tweets
        """
    elif question_id == 12:
        # 12. Find top users with most retweeted tweets
        query = """
        MATCH (t1:Tweet)-[:RETWEETS]->(t2:Tweet)<-[:PUBLISH]-(u:User)
        RETURN u.username, count(t1) as retweets_received
        ORDER BY retweets_received DESC LIMIT 10
        """
    elif question_id == 13:
        # 13. Find top most mentioned users
        query = """
        MATCH (t:Tweet)-[:MENTIONS]->(u:User)
        RETURN u.username, count(t) as mentions
        ORDER BY mentions DESC LIMIT 10
        """
    elif question_id == 14:
        # 14. Find most followed users
        query = """
        MATCH (u1:User)-[:FOLLOWS]->(u2:User)
        RETURN u2.username, count(u1) as followers
        ORDER BY followers DESC LIMIT 10
        """
    elif question_id == 15:
        # 15. Find users following the most people
        query = """
        MATCH (u1:User)-[:FOLLOWS]->(u2:User)
        RETURN u1.username, count(u2) as following
        ORDER BY following DESC LIMIT 10
        """
    else:
        return {"error": "Invalid question ID"}

    with driver.session() as session:
        result = session.run(query)
        return [record.data() for record in result]
