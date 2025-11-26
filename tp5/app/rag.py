import os
from langchain_community.document_loaders import WikipediaLoader
from langchain_experimental.graph_transformers.diffbot import DiffbotGraphTransformer
from langchain_community.graphs import Neo4jGraph
from langchain_ollama import ChatOllama
from langchain.chains import GraphCypherQAChain
from langchain.prompts import PromptTemplate

class RAGManager:
    def __init__(self):
        self.diffbot_api_key = os.getenv("DIFFBOT_API_KEY")
        self.ollama_base_url = os.getenv("OLLAMA_BASE_URL", "http://host.docker.net:11434")
        self.neo4j_url = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
        self.neo4j_user = os.getenv("NEO4J_USER", "neo4j")
        self.neo4j_password = os.getenv("NEO4J_PASSWORD", "password")
        
        self.graph = Neo4jGraph(
            url=self.neo4j_url, 
            username=self.neo4j_user, 
            password=self.neo4j_password
        )
        
        self.llm = ChatOllama(
            model="llama3:8b",
            base_url=self.ollama_base_url,
            temperature=0
        )

    def load_wikipedia_article(self, query="Satoshi Nakamoto", load_max_docs=1):
        print(f"Loading Wikipedia article for: {query}")
        raw_documents = WikipediaLoader(query=query, load_max_docs=load_max_docs).load()
        print(f"Loaded {len(raw_documents)} documents.")
        return raw_documents

    def extract_and_ingest(self, documents):
        if not self.diffbot_api_key or "INSERT" in self.diffbot_api_key:
            print("WARNING: Valid DIFFBOT_API_KEY not found. Skipping extraction.")
            print("Please set DIFFBOT_API_KEY in docker-compose.yml or .env")
            return

        print("Extracting graph data with Diffbot...")
        diffbot_nlp = DiffbotGraphTransformer(diffbot_api_key=self.diffbot_api_key)
        graph_documents = diffbot_nlp.convert_to_graph_documents(documents)
        
        print(f"Extracted {len(graph_documents)} graph documents.")
        print("Ingesting into Neo4j...")
        self.graph.add_graph_documents(graph_documents)
        print("Ingestion complete.")

    def create_qa_chain(self):
        print("Creating Graph QA Chain...")
        
        # Custom Cypher Generation Prompt
        cypher_generation_template = """Task:Generate Cypher statement to query a graph database.
Instructions:
Use only the provided relationship types and properties in the schema.
Do not use any other relationship types or properties that are not provided.
Schema:
{schema}
Note: Do not include any explanations or apologies in your responses.
Do not respond to any questions that might ask anything else than for you to construct a Cypher statement.
Do not include any text except the generated Cypher statement.
Examples: Here are a few examples of generated Cypher statements for particular questions:
# How many people played in Top Gun?
MATCH (m:Movie {{title:"Top Gun"}})<-[:ACTED_IN]-(p:Person) RETURN count(p)

The question is:
{question}"""

        cypher_prompt = PromptTemplate(
            input_variables=["schema", "question"], 
            template=cypher_generation_template
        )

        return GraphCypherQAChain.from_llm(
            self.llm,
            graph=self.graph,
            cypher_prompt=cypher_prompt,
            verbose=True,
            allow_dangerous_requests=True
        )

    def ask_question(self, chain, question):
        print(f"\nQuestion: {question}")
        try:
            response = chain.invoke(question)
            print(f"Answer: {response['result']}")
        except Exception as e:
            print(f"Error answering question: {e}")
