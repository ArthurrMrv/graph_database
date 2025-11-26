"""Core package for the knowledge graph recommendation demo."""

from .data import Dataset, load_dataset
from .graph import GraphData, build_graph, display_graph
from .recommendation import RecommendationEngine, Recommendation

__all__ = [
    "Dataset",
    "GraphData",
    "Recommendation",
    "RecommendationEngine",
    "load_dataset",
    "build_graph",
    "display_graph"
]
