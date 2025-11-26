# TP3: Twitter Network Analysis with Neo4j GDS

This project implements a comprehensive analysis of a Twitter network using Neo4j and the Graph Data Science (GDS) plugin.

## Features

### Data Loading
- Create unique constraints for User and Tweet nodes
- Import users, followers, tweets, mentions, retweets, and replies from CSV files
- Batch processing for large datasets

### Graph Projections
- Create directed graph projections for GDS algorithms
- Create undirected graph projections for path finding
- List and inspect graph projections

### GDS Algorithms
- **PageRank**: Measure user importance based on followers and their importance
- **Betweenness Centrality**: Identify bridge users connecting different network parts
- **Degree Centrality**: Calculate in-degree (followers) and out-degree (following)
- **Community Detection (Louvain)**: Find groups of users who follow each other
- **Triangle Counting**: Measure network clustering and cohesion
- **Shortest Path (Dijkstra)**: Find paths between users

### Analysis Queries
- Find random users, relationships, and tweets
- Count relationships by type
- Analyze tweet distribution by year
- Find most followed and most following users
- Identify most mentioned users
- Compare retweets and original tweets
- Find users mentioned but without tweets
- Calculate missing data ratios


## Usage

## Data Sources

The script loads data from the following CSV URLs:
- Users: `https://bit.ly/39JYakC`
- Followers: `https://bit.ly/3n08lEL`
- Tweets: `https://bit.ly/3y3ODyc`
- Mentions: `https://bit.ly/3tINZ6D`
- Retweets: `https://bit.ly/3QyDrRl`
- Replies: `https://bit.ly/3b9Wgdx`

## Graph Schema

- **Nodes**:
  - `User`: Users with properties `id`, `username`, `name`, `registeredAt`
  - `Tweet`: Tweets with properties `id`, `text`, `createdAt`

- **Relationships**:
  - `FOLLOWS`: User follows another User
  - `PUBLISH`: User publishes a Tweet
  - `MENTIONS`: Tweet mentions a User
  - `RETWEETS`: Tweet retweets another Tweet
  - `IN_REPLY_TO`: Tweet replies to another Tweet

## Practice Questions Answered

The code includes implementations for all practice questions:
1. Find five random user nodes
2. Find five random FOLLOWS relationships
3. Find the text property of three random Tweet nodes
4. Visualize sample RETWEETS relationships
5. Calculate missing values ratio for createdAt
6. Count relationships by type
7. Compare retweet and original tweet text
8. Calculate tweet distribution by year
9. Find tweets created in 2021
10. Return top days with highest tweet count
11. Count users mentioned without tweets
12. Find top users with most retweeted tweets
13. Find top most mentioned users
14. Find most followed users
15. Find users following the most people

## Notes

- **MERGE vs CREATE**: The code uses `MERGE` instead of `CREATE` to ensure idempotency. This prevents duplicate nodes/relationships when running the import multiple times.
- **Batch Processing**: For large datasets, consider using `CALL { ... } IN TRANSACTIONS` for better performance.
- **GDS Plugin**: Make sure the Neo4j GDS plugin is installed and enabled in your Neo4j instance.

## License

This project is part of a course assignment.

