# TP4: Introduction to Graph ML - Predict Nodes Inside Graph Network

This project implements a Twitch streamer language prediction system using graph machine learning techniques.

## Overview

The goal is to predict the language of new Twitch streamers based on their shared audience with other streamers. The approach uses:

1. **Graph Construction**: Create a monopartite graph where nodes are streams and edges represent shared audience
2. **Node Embeddings**: Use Node2Vec algorithm to generate embeddings for each stream
3. **Classification**: Train a RandomForest classifier to predict stream language from embeddings

## Setup

### Prerequisites

- Neo4j database running (via Docker or local installation)
- Neo4j GDS (Graph Data Science) plugin installed
- Python 3.8+



The script loads two CSV files:
- **Streamer CSV**: Contains stream information (streamId, language)
  - URL: `https://bit.ly/3JjgKgZ`
- **Audience CSV**: Contains user-stream relationships (userId, streamId)
  - Note: Update the URL in the code with the actual audience CSV URL

## Usage

The script will:
1. Connect to Neo4j
2. Create constraints and load data
3. Create graph projections
4. Run Node2Vec algorithm
5. Analyze embeddings (Euclidean vs Cosine distances)
6. Train and evaluate RandomForest classifier
7. Generate visualizations

## Output

The script generates several visualizations saved in the `tp4/` directory:
- `embedding_distances.png`: Distribution of Euclidean and Cosine distances
- `degree_cosine.png`: Degree distribution by cosine similarity
- `weight_cosine.png`: Cosine similarity vs average weight
- `confusion_matrix.png`: Confusion matrix for language prediction

## Key Concepts

### Undirected Graph Projection
The graph is undirected because if stream A shares audience with stream B, then stream B also shares audience with stream A (symmetric relationship).

### Node2Vec Parameters
- `embeddingDimension`: 8 (dimension of the embedding vectors)
- `relationshipWeightProperty`: 'weight' (uses shared audience count)
- `inOutFactor`: 0.5 (balance between BFS and DFS exploration)
- `returnFactor`: 1 (probability of returning to previous node)

### Evaluation Metrics
- **F1-score (weighted average)** is the most appropriate metric because:
  - It balances precision and recall
  - Accounts for class imbalance
  - More informative than accuracy alone

## Questions & Answers

1. **What do you think about the confusion matrix?**
   - Shows prediction accuracy per language
   - Diagonal elements = correct predictions
   - Off-diagonal = misclassifications
   - Some languages may be confused more often

2. **Appropriate metric for manager?**
   - F1-score (weighted average) is recommended
   - Balances precision and recall
   - Accounts for class imbalance

3. **How to improve classifier quality?**
   - Increase embedding dimension
   - Tune Node2Vec parameters
   - Use more RandomForest trees
   - Try other classifiers (XGBoost, SVM, Neural Networks)
   - Handle class imbalance
   - Add graph features (degree, centrality)
   - Use ensemble methods

## Notes

- The audience CSV URL in the code may need to be updated with the actual URL
- For large datasets, APOC plugin is recommended for batch processing
- The script includes error handling for missing APOC plugin

