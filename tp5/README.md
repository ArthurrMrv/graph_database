# TP5: Introduction to Local Knowledge Graph

Building a local Knowledge Graph RAG with Neo4j, LangChain, and Ollama.

## Overview

This project demonstrates how to:
- Extract entities and relationships from Wikipedia articles using Diffbot API
- Load graph data into Neo4j
- Query and explore the knowledge graph
- Use LangChain and Ollama to create a natural language query interface
- Generate entity summaries from graph data

## Prerequisites

- Python 3.10+
- Neo4j available locally (via Docker)
- Ollama running locally with a model pulled (e.g., `llama3:8b`)
- A FREE Diffbot API key (set as `DIFFBOT_API_KEY` environment variable)

## Setup

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Start Neo4j with Docker

```bash
docker-compose up -d
```

This will start Neo4j with:
- APOC plugin
- Graph Data Science plugin
- Bloom visualization
- Default database: `shop`
- Credentials: `neo4j/password`

### 4. Start Ollama and pull a model

In one terminal:
```bash
ollama serve
```

In another terminal:
```bash
ollama pull llama3:8b
```

### 5. Configure environment variables

Create a `.env` file in the project root:

```env
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=password
DIFFBOT_API_KEY=your_diffbot_api_key_here
OLLAMA_MODEL=llama3:8b
```

**⚠️ Important: Do NOT commit your Diffbot API key to version control!**

## Usage

## Features

### 1. Wikipedia Article Loading
- Loads articles using LangChain's `WikipediaLoader`
- Supports any topic or person (default: "Satoshi Nakamoto")

### 2. Graph Extraction
- Uses Diffbot API to extract entities and relationships
- Automatically splits long documents
- Returns structured graph documents

### 3. Neo4j Ingestion
- Cleans database before loading
- Ingests graph documents into Neo4j
- Preserves node and relationship properties

### 4. Graph Exploration
- Lists all node labels and relationship types
- Counts nodes and relationships
- Queries specific entity relationships
- Explores top connections, interests, employment, locations

### 5. Natural Language Querying
- Creates `GraphCypherQAChain` with custom Cypher prompt
- Uses Ollama for local LLM inference
- Supports natural language questions about the graph

### 6. Entity Summarization
- Extracts subgraphs around entities
- Builds structured context
- Generates summaries using LLM

## Key Functions

### `load_wikipedia_article(query, load_max_docs)`
Loads Wikipedia articles for a given query.

### `extract_graph_with_diffbot(documents, api_key)`
Extracts entities and relationships using Diffbot API.

### `ingest_graph_to_neo4j(graph_documents)`
Loads graph data into Neo4j.

### `explore_graph_schema()`
Lists all node labels and relationship types in the graph.

### `query_person_relationships(person_name)`
Queries all relationships for a specific person.

### `get_all_relationships_for_person(person_name)`
Reusable function to get all relationships (incoming and outgoing).

### `create_graph_cypher_qa_chain()`
Creates a GraphCypherQAChain with custom prompt and Ollama.

### `ask_graph(question, chain)`
Helper function to query the graph with natural language.

### `get_entity_subgraph(entity_name, max_depth, max_nodes)`
Extracts a subgraph around a specific entity.

### `summarize_entity(entity_name, subgraph, chain)`
Generates a summary of an entity based on its subgraph.

## Custom Cypher Prompt Elements

The custom prompt helps the model generate correct Cypher by:
- **Schema information**: Provides structure of nodes and relationships
- **Property hints**: Emphasizes `.name` property for Person nodes
- **Relationship unions**: Shows how to use multiple relationship types
- **Query structure**: Guides MATCH, WHERE, RETURN pattern
- **Filtering guidelines**: Shows how to filter by properties

## Improving Summary Quality

To improve entity summaries and avoid hallucinations:

1. **Add temporal information**: Include dates from graph properties
2. **Include relationship context**: Add weights and properties
3. **Add node properties**: Include descriptions, types, etc.
4. **Constrain LLM**: Only use provided graph data
5. **Use few-shot examples**: Show examples of good summaries
6. **Add validation**: Check facts against graph

## Handling Multiple Entity Nodes

If multiple nodes exist for the same entity name:
- Use the node with the most connections
- Use the node with matching ID if available
- Use the node with the most specific labels
- Filter by additional properties (type, source, etc.)

## Troubleshooting

### Dependency Conflicts
If you see langchain-core incompatibilities:
- Pin versions in `requirements.txt`
- Align versions across all langchain packages
- Use a fresh virtual environment

### Empty Query Results
- Verify node labels and properties match extractor output
- Check that `.name` property exists on Person nodes
- Use case-insensitive searches
- Inspect the graph schema first

### Ollama Issues
- Ensure `ollama serve` is running
- Verify model is pulled: `ollama list`
- Try a larger model if responses are weak
- Adjust temperature and other parameters

### Neo4j Connection Issues
- Verify Neo4j is running: `docker ps`
- Check credentials in `.env` file
- Verify port 7687 is accessible
- Check Neo4j logs: `docker-compose logs neo4j`

## Example Questions

- "Who is Satoshi Nakamoto?"
- "What organizations is Satoshi Nakamoto associated with?"
- "What are the main relationships connected to Satoshi Nakamoto?"
- "Who founded Bitcoin?"
- "What are the interests of Satoshi Nakamoto?"

## Notes

- The Diffbot API has rate limits for free accounts
- Large documents are automatically split for processing
- Graph extraction may take time depending on document size
- Ollama responses depend on model size and quality
- Customize the topic by changing the Wikipedia query

## License

This is an educational project for learning graph databases and RAG systems.

