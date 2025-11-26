import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity, euclidean_distances
import os

class Visualizer:
    def __init__(self, driver):
        self.driver = driver
        self.output_dir = "/work/output"
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    def generate_plots(self):
        print("Generating visualizations...")
        with self.driver.session() as session:
            # 1. Fetch Embeddings for Distance Analysis (Sample)
            result = session.run("""
                MATCH (s:Stream)
                WHERE s.embedding IS NOT NULL
                RETURN s.embedding as embedding
                LIMIT 1000
            """)
            embeddings = [record["embedding"] for record in result]
            
            if embeddings:
                embeddings_matrix = np.array(embeddings)
                
                # Calculate distances
                cosine_sim = cosine_similarity(embeddings_matrix)
                # Convert similarity to distance (1 - similarity)
                cosine_dist = 1 - cosine_sim
                euclidean_dist = euclidean_distances(embeddings_matrix)
                
                # Plot Distributions
                plt.figure(figsize=(12, 6))
                sns.histplot(cosine_dist.flatten(), color="blue", label="Cosine Distance", kde=True, stat="density", alpha=0.5)
                sns.histplot(euclidean_dist.flatten(), color="red", label="Euclidean Distance", kde=True, stat="density", alpha=0.5)
                plt.legend()
                plt.title("Distribution of Embedding Distances")
                plt.savefig(f"{self.output_dir}/embedding_distances.png")
                print("Saved embedding_distances.png")

            # 2. Degree vs Cosine Similarity
            # We need pairs of nodes, their degree, and their cosine similarity
            # This is expensive to do for all, so we sample or use a specific query
            # For simplicity, let's fetch node degrees and average cosine similarity of neighbors
            
            # Actually, let's stick to the README requirements:
            # "degree_cosine.png": Degree distribution by cosine similarity
            # "weight_cosine.png": Cosine similarity vs average weight
            
            # Let's fetch relationships with weights and embeddings of source/target
            result = session.run("""
                MATCH (s1:Stream)-[r:SHARED_AUDIENCE]->(s2:Stream)
                WHERE s1.embedding IS NOT NULL AND s2.embedding IS NOT NULL
                RETURN r.weight as weight, s1.embedding as emb1, s2.embedding as emb2
                LIMIT 2000
            """)
            
            data = [record.data() for record in result]
            if data:
                weights = []
                cos_sims = []
                
                for row in data:
                    w = row['weight']
                    e1 = np.array(row['emb1']).reshape(1, -1)
                    e2 = np.array(row['emb2']).reshape(1, -1)
                    sim = cosine_similarity(e1, e2)[0][0]
                    
                    weights.append(w)
                    cos_sims.append(sim)
                
                # Plot Weight vs Cosine
                plt.figure(figsize=(10, 6))
                sns.scatterplot(x=weights, y=cos_sims, alpha=0.5)
                plt.xlabel("Shared Audience Weight")
                plt.ylabel("Cosine Similarity")
                plt.title("Cosine Similarity vs Shared Audience Weight")
                plt.savefig(f"{self.output_dir}/weight_cosine.png")
                print("Saved weight_cosine.png")
