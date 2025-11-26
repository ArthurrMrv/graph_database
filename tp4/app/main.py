import os
from neo4j import GraphDatabase
from loader import Loader
from graph import GraphManager
from ml import MLManager
from viz import Visualizer

# Configuration
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

def main():
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    
    try:
        # 1. Load Data
        loader = Loader(driver)
        loader.load_data()
        
        # 2. Graph Operations (Projection + Node2Vec)
        graph_mgr = GraphManager(driver)
        graph_mgr.create_projection()
        graph_mgr.run_node2vec()
        
        # 3. Visualization
        viz = Visualizer(driver)
        viz.generate_plots()
        
        # 4. Machine Learning
        ml_mgr = MLManager(driver)
        ml_mgr.train_and_evaluate()
        
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        driver.close()

if __name__ == "__main__":
    main()
