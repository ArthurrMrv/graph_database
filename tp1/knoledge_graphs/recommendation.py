"""Recommendation algorithms operating on the derived graph."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Mapping, Set, Tuple

from .graph import GraphData


STRATEGY_WEIGHTS: Mapping[str, float] = {
    "co_occurrence": 0.4,
    "similarity": 0.3,
    "personalized_pagerank": 0.3,
}


@dataclass(frozen=True)
class Recommendation:
    """Represents an ordered recommendation with detailed attribution."""

    product_id: str
    score: float
    contributions: Mapping[str, float]


class RecommendationEngine:
    """Compute product recommendations using multiple graph strategies."""

    def __init__(
        self,
        graph: GraphData,
        *,
        damping: float = 0.85,
        tolerance: float = 1e-6,
        max_iterations: int = 50,
    ) -> None:
        self.graph = graph
        self.damping = damping
        self.tolerance = tolerance
        self.max_iterations = max_iterations
        self._product_ids: List[str] = list(graph.product_adjacency.keys())
        self._global_pagerank: Dict[str, float] = self._run_pagerank(self._uniform_personalization())

    def recommend_for_customer(self, customer_id: str, top_n: int = 3) -> List[Recommendation]:
        """Return top-N products tailored for the given customer."""

        self._ensure_known_customer(customer_id)
        purchased = set(self.graph.customer_products.get(customer_id, set()))
        interacted = self._interacted_products(customer_id)
        seeds = purchased or interacted
        if not seeds:
            return self._fallback_top_pagerank(top_n)

        strategies = {
            "co_occurrence": self._normalize_scores(
                self._co_occurrence_scores(seeds)
            ),
            "similarity": self._normalize_scores(
                self._similarity_scores(seeds)
            ),
            "personalized_pagerank": self._normalize_scores(
                self._personalized_pagerank(seeds)
            ),
        }

        exclude = purchased.union(interacted)
        combined = self._combine_strategies(strategies, exclude)
        return combined[:top_n]

    def strategy_breakdown(self, customer_id: str, top_n: int = 3) -> Dict[str, List[Tuple[str, float]]]:
        """Expose per-strategy rankings for inspection and testing."""

        self._ensure_known_customer(customer_id)
        purchased = set(self.graph.customer_products.get(customer_id, set()))
        interacted = self._interacted_products(customer_id)
        seeds = purchased or interacted
        if not seeds:
            return {
                "global_pagerank": self._top_items(self._global_pagerank, top_n, exclude=set()),
            }

        return {
            name: self._top_items(scores, top_n, exclude=purchased.union(interacted))
            for name, scores in {
                "co_occurrence": self._co_occurrence_scores(seeds),
                "similarity": self._similarity_scores(seeds),
                "personalized_pagerank": self._personalized_pagerank(seeds),
            }.items()
        }

    # -- helpers ---------------------------------------------------------

    def _ensure_known_customer(self, customer_id: str) -> None:
        if customer_id not in self.graph.dataset.customers:
            raise ValueError(f"Unknown customer: {customer_id}")

    def _interacted_products(self, customer_id: str) -> Set[str]:
        interacted = {
            product_id
            for (cid, product_id), weight in self.graph.event_weights.items()
            if cid == customer_id and weight > 0
        }
        return interacted

    def _co_occurrence_scores(self, seeds: Set[str]) -> Dict[str, float]:
        scores: Dict[str, float] = {}
        for seed in seeds:
            for candidate, weight in self.graph.product_cooccurrence.get(seed, {}).items():
                if candidate in seeds:
                    continue
                scores[candidate] = scores.get(candidate, 0.0) + float(weight)
        return scores

    def _similarity_scores(self, seeds: Set[str]) -> Dict[str, float]:
        scores: Dict[str, float] = {}
        for candidate, candidate_customers in self.graph.product_customers.items():
            if candidate in seeds:
                continue
            accumulator = 0.0
            for seed in seeds:
                seed_customers = self.graph.product_customers.get(seed, set())
                if not seed_customers and not candidate_customers:
                    continue
                union = seed_customers | candidate_customers
                if not union:
                    continue
                intersection = seed_customers & candidate_customers
                if not intersection:
                    continue
                accumulator += len(intersection) / len(union)
            if accumulator > 0:
                scores[candidate] = accumulator
        return scores

    def _personalized_pagerank(self, seeds: Set[str]) -> Dict[str, float]:
        personalization = {product_id: 0.0 for product_id in self._product_ids}
        if not seeds:
            return self._global_pagerank
        seed_weight = 1.0 / len(seeds)
        for seed in seeds:
            personalization[seed] = seed_weight
        return self._run_pagerank(personalization)

    def _uniform_personalization(self) -> Dict[str, float]:
        if not self._product_ids:
            return {}
        weight = 1.0 / len(self._product_ids)
        return {product_id: weight for product_id in self._product_ids}

    def _run_pagerank(self, personalization: Mapping[str, float]) -> Dict[str, float]:
        if not self._product_ids:
            return {}
        ranks = {product_id: 1.0 / len(self._product_ids) for product_id in self._product_ids}
        personalization = self._normalize_personalization(personalization)
        for _ in range(self.max_iterations):
            new_ranks = {
                product_id: (1.0 - self.damping) * personalization.get(product_id, 0.0)
                for product_id in self._product_ids
            }
            sink_rank = sum(
                ranks[product_id]
                for product_id in self._product_ids
                if not self.graph.product_adjacency.get(product_id)
            )
            leak = self.damping * sink_rank / len(self._product_ids)
            for product_id in self._product_ids:
                neighbors = self.graph.product_adjacency.get(product_id, {})
                if not neighbors:
                    continue
                for neighbor, weight in neighbors.items():
                    new_ranks[neighbor] += self.damping * ranks[product_id] * weight
            if leak:
                for product_id in self._product_ids:
                    new_ranks[product_id] += leak
            delta = sum(abs(new_ranks[pid] - ranks[pid]) for pid in self._product_ids)
            ranks = new_ranks
            if delta < self.tolerance:
                break
        return ranks

    def _normalize_personalization(self, values: Mapping[str, float]) -> Dict[str, float]:
        total = sum(values.get(product_id, 0.0) for product_id in self._product_ids)
        if total == 0:
            return self._uniform_personalization()
        return {
            product_id: values.get(product_id, 0.0) / total
            for product_id in self._product_ids
        }

    def _normalize_scores(self, scores: Mapping[str, float]) -> Dict[str, float]:
        if not scores:
            return {}
        max_value = max(scores.values())
        if max_value == 0:
            return {key: 0.0 for key in scores}
        return {key: value / max_value for key, value in scores.items()}

    def _combine_strategies(
        self,
        strategies: Mapping[str, Mapping[str, float]],
        exclude: Set[str],
    ) -> List[Recommendation]:
        combined: Dict[str, Dict[str, float]] = {}
        for name, scores in strategies.items():
            weight = STRATEGY_WEIGHTS.get(name, 0.0)
            if weight == 0:
                continue
            for product_id, value in scores.items():
                if product_id in exclude or value <= 0:
                    continue
                product_scores = combined.setdefault(product_id, {})
                product_scores[name] = value * weight
        recommendations = [
            Recommendation(
                product_id=product_id,
                score=sum(contributions.values()),
                contributions=contributions,
            )
            for product_id, contributions in combined.items()
        ]
        recommendations.sort(key=lambda rec: (-rec.score, rec.product_id))
        return recommendations

    def _fallback_top_pagerank(self, top_n: int) -> List[Recommendation]:
        top_items = self._top_items(self._global_pagerank, top_n, exclude=set())
        return [
            Recommendation(product_id=product_id, score=score, contributions={"global_pagerank": score})
            for product_id, score in top_items
        ]

    def _top_items(
        self,
        scores: Mapping[str, float],
        top_n: int,
        *,
        exclude: Set[str],
    ) -> List[Tuple[str, float]]:
        ranked = [item for item in scores.items() if item[0] not in exclude]
        ranked.sort(key=lambda item: (-item[1], item[0]))
        return ranked[:top_n]
