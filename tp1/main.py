"""Executable entry point for the knowledge graph recommendation demo."""
from __future__ import annotations

from knoledge_graphs import RecommendationEngine, build_graph, load_dataset, display_graph
from knoledge_graphs.api import create_app

DATASET = load_dataset()
GRAPH = build_graph(DATASET)
ENGINE = RecommendationEngine(GRAPH)
app = create_app(ENGINE)


def main() -> None:
    """Showcase a simple recommendation call for manual execution."""

    example_customer = next(iter(DATASET.customer_ids()))
    recommendations = ENGINE.recommend_for_customer(example_customer, top_n=3)
    
    print(f"Recommendations for {example_customer}:")
    
    if len(recommendations) == 0:
        print("No recommendations found with Dataset : ")
        display_graph(GRAPH)
        return
    
    for recommendation in recommendations:
        contribution = ", ".join(
            f"{name}={value:.3f}" for name, value in recommendation.contributions.items()
        )
        print(f" - {recommendation.product_id}: {recommendation.score:.3f} ({contribution})")


if __name__ == "__main__":
    main()
