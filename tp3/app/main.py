from fastapi import FastAPI
from database import db
from loader import load_data
from gds import (
    create_projections,
    run_pagerank,
    run_betweenness,
    run_degree_centrality,
    run_louvain,
    run_triangle_count,
    run_dijkstra
)
from queries import get_question_answer

app = FastAPI()

@app.on_event("startup")
def startup():
    db.connect()

@app.on_event("shutdown")
def shutdown():
    db.close()

@app.get("/")
def read_root():
    return {"message": "TP3 Twitter Network Analysis API"}

@app.post("/load-data")
def trigger_load_data():
    return load_data()

@app.post("/create-projections")
def trigger_create_projections():
    return create_projections()

@app.get("/analysis/pagerank")
def get_pagerank():
    return run_pagerank()

@app.get("/analysis/betweenness")
def get_betweenness():
    return run_betweenness()

@app.get("/analysis/degree")
def get_degree():
    return run_degree_centrality()

@app.get("/analysis/louvain")
def get_louvain():
    return run_louvain()

@app.get("/analysis/triangle-count")
def get_triangle_count():
    return run_triangle_count()

@app.get("/analysis/shortest-path")
def get_shortest_path(start_user: str, end_user: str):
    return run_dijkstra(start_user, end_user)

@app.get("/questions/{question_id}")
def get_question(question_id: int):
    return get_question_answer(question_id)
