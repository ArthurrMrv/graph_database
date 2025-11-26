"""FastAPI application exposing the recommendation pipeline."""
from __future__ import annotations

from typing import Dict, List

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from .recommendation import Recommendation, RecommendationEngine


class RecommendationItem(BaseModel):
    """Serializable recommendation payload."""

    product_id: str
    score: float
    contributions: Dict[str, float]


class RecommendationResponse(BaseModel):
    """Payload returned by the recommendation endpoint."""

    customer_id: str
    recommendations: List[RecommendationItem]


class StrategyEntry(BaseModel):
    """Single item inside the per-strategy ranking."""

    product_id: str
    score: float


class StrategyBreakdownResponse(BaseModel):
    """Detailed per-strategy ranking data for debugging and testing."""

    customer_id: str
    strategies: Dict[str, List[StrategyEntry]]


def _to_response_items(recommendations: List[Recommendation]) -> List[RecommendationItem]:
    """Convert internal recommendation objects into API models."""

    return [
        RecommendationItem(
            product_id=item.product_id,
            score=round(item.score, 6),
            contributions={k: round(v, 6) for k, v in item.contributions.items()},
        )
        for item in recommendations
    ]


def create_app(engine: RecommendationEngine) -> FastAPI:
    """Build the FastAPI application bound to a recommendation engine."""

    app = FastAPI(title="Knowledge Graph Recommendations", version="1.0.0")

    @app.get("/health")
    def health() -> Dict[str, bool]:
        """Basic liveness probe used by the README instructions."""

        return {"ok": True}

    @app.get("/customers/{customer_id}/recommendations", response_model=RecommendationResponse)
    def customer_recommendations(customer_id: str, top_n: int = Query(3, ge=1, le=10)) -> RecommendationResponse:
        """Return ranked recommendations for the requested customer."""

        try:
            recommendations = engine.recommend_for_customer(customer_id=customer_id, top_n=top_n)
        except ValueError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error
        return RecommendationResponse(
            customer_id=customer_id,
            recommendations=_to_response_items(recommendations),
        )

    @app.get("/customers/{customer_id}/strategies", response_model=StrategyBreakdownResponse)
    def customer_strategy_breakdown(
        customer_id: str,
        top_n: int = Query(3, ge=1, le=10),
    ) -> StrategyBreakdownResponse:
        """Expose individual strategy rankings for experimentation."""

        try:
            breakdown = engine.strategy_breakdown(customer_id=customer_id, top_n=top_n)
        except ValueError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error
        serializable = {
            name: [
                StrategyEntry(product_id=product_id, score=round(score, 6))
                for product_id, score in entries
            ]
            for name, entries in breakdown.items()
        }
        return StrategyBreakdownResponse(customer_id=customer_id, strategies=serializable)

    return app
