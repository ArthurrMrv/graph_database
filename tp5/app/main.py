import os
from neo4j import GraphDatabase
from rag import RAGManager
from graph_utils import GraphUtils

def main():
    print("Initializing TP5 Application...")
    
    rag_manager = RAGManager()
    
    # 1. Load Data
    documents = rag_manager.load_wikipedia_article("Satoshi Nakamoto")
    
    # 2. Extract and Ingest (requires Diffbot Key)
    rag_manager.extract_and_ingest(documents)
    
    # 3. Explore Graph
    # Create a direct driver for utility functions
    driver = GraphDatabase.driver(
        os.getenv("NEO4J_URI", "bolt://neo4j:7687"),
        auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD", "password"))
    )
    utils = GraphUtils(driver)
    utils.explore_graph_schema()
    
    # 4. QA Chain
    chain = rag_manager.create_qa_chain()
    
    # 5. Ask Questions
    questions = [
        "Who is Satoshi Nakamoto?",
        "What organizations is Satoshi Nakamoto associated with?",
        "Who founded Bitcoin?"
    ]
    
    for q in questions:
        rag_manager.ask_question(chain, q)
        
    driver.close()

if __name__ == "__main__":
    main()
