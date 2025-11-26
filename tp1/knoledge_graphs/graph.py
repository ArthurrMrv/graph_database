"""Graph-building utilities that mimic the ETL step into Neo4j."""
from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from itertools import combinations
from typing import DefaultDict, Dict, Iterable, Mapping, Set, Tuple

from .data import Dataset


EVENT_WEIGHTS: Mapping[str, float] = {
    "view": 0.5,
    "click": 1.0,
    "add_to_cart": 2.0,
}


@dataclass
class GraphData:
    """Holds derived structures that behave like a projected product graph."""

    dataset: Dataset
    product_cooccurrence: Dict[str, Counter[str]]
    product_customers: Dict[str, Set[str]]
    customer_products: Dict[str, Set[str]]
    event_weights: Dict[Tuple[str, str], float]
    product_adjacency: Dict[str, Dict[str, float]]

    def neighbors(self, product_id: str) -> Dict[str, float]:
        """Return normalized adjacency weights for a product."""

        return self.product_adjacency.get(product_id, {})


def _initial_adjacency(product_ids: Iterable[str]) -> Dict[str, Dict[str, float]]:
    """Create a zeroed adjacency mapping for all products."""

    return {pid: {} for pid in product_ids}


def build_graph(dataset: Dataset) -> GraphData:
    """Construct an in-memory graph representation from the dataset.

    The implementation mirrors the ETL plans from the README by computing the
    relationships that would normally be expressed in Neo4j: product
    co-occurrence, customer-product interaction edges, and interaction weights.
    """

    product_cooccurrence: DefaultDict[str, Counter[str]] = defaultdict(Counter)
    product_customers: DefaultDict[str, Set[str]] = defaultdict(set)
    customer_products: DefaultDict[str, Set[str]] = defaultdict(set)
    event_weights: DefaultDict[Tuple[str, str], float] = defaultdict(float)

    for order in dataset.orders.values():
        products_in_order = [item.product_id for item in order.items]
        for product_id in products_in_order:
            product_customers[product_id].add(order.customer_id)
            customer_products[order.customer_id].add(product_id)
        for left, right in combinations(products_in_order, 2):
            product_cooccurrence[left][right] += 1
            product_cooccurrence[right][left] += 1

    for event in dataset.events:
        product_customers[event.product_id].add(event.customer_id)
        customer_products[event.customer_id].add(event.product_id)
        weight = EVENT_WEIGHTS.get(event.event_type, 0.0)
        event_weights[(event.customer_id, event.product_id)] += weight

    adjacency: Dict[str, Dict[str, float]] = _initial_adjacency(dataset.product_ids())
    for product_id, neighbors in product_cooccurrence.items():
        total = sum(neighbors.values())
        if total == 0:
            continue
        adjacency[product_id] = {
            neighbor_id: count / total for neighbor_id, count in neighbors.items()
        }

    return GraphData(
        dataset=dataset,
        product_cooccurrence={k: Counter(v) for k, v in product_cooccurrence.items()},
        product_customers={k: set(v) for k, v in product_customers.items()},
        customer_products={k: set(v) for k, v in customer_products.items()},
        event_weights=dict(event_weights),
        product_adjacency=adjacency,
    )
    
def display_graph(graph_data):
    """
    Visualize the product co-occurrence graph using networkx and matplotlib.

    Nodes: Products
    Edges: Co-occurrence (thicker = more frequent)
    """
    try:
        import networkx as nx
        import matplotlib.pyplot as plt
    except ImportError:
        raise ImportError(
            "To use display_graph, please install 'networkx' and 'matplotlib'."
        )

    G = nx.Graph()
    # Add nodes
    for product_id in graph_data.product_adjacency.keys():
        G.add_node(product_id)
    # Add edges with weights
    for product_id, neighbors in graph_data.product_cooccurrence.items():
        for neighbor_id, count in neighbors.items():
            if G.has_edge(product_id, neighbor_id):
                continue  # Avoid double-adding undirected edges
            G.add_edge(product_id, neighbor_id, weight=count)

    pos = nx.spring_layout(G, seed=42)
    weights = [G[u][v]['weight'] for u, v in G.edges()]
    # Normalize edge widths for visibility
    max_weight = max(weights) if weights else 1
    widths = [2 + 6 * (w / max_weight) for w in weights]

    plt.figure(figsize=(10, 8))
    nx.draw_networkx_nodes(G, pos, node_size=400, node_color='skyblue')
    nx.draw_networkx_edges(G, pos, width=widths, alpha=0.6)
    nx.draw_networkx_labels(G, pos, font_size=10, font_color='black')
    plt.title("Product Co-occurrence Graph")
    plt.axis('off')
    plt.tight_layout()
    plt.show()
