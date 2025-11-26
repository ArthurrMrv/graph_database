"""Data definitions and loading utilities for the recommendation demo."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Dict, Iterable, List


@dataclass(frozen=True)
class Customer:
    """Represents a customer in the transactional store."""

    id: str
    name: str
    join_date: date


@dataclass(frozen=True)
class Category:
    """Represents a product category."""

    id: str
    name: str


@dataclass(frozen=True)
class Product:
    """Represents a purchasable product."""

    id: str
    name: str
    price: float
    category_id: str


@dataclass(frozen=True)
class OrderItem:
    """Represents a single product line inside an order."""

    product_id: str
    quantity: int


@dataclass
class Order:
    """Represents a placed order with its items."""

    id: str
    customer_id: str
    placed_at: datetime
    items: List[OrderItem] = field(default_factory=list)

    def add_item(self, product_id: str, quantity: int) -> None:
        """Append an item to the order."""

        self.items.append(OrderItem(product_id=product_id, quantity=quantity))


@dataclass(frozen=True)
class Event:
    """Represents an interaction between a customer and a product."""

    id: str
    customer_id: str
    product_id: str
    event_type: str
    occurred_at: datetime


@dataclass
class Dataset:
    """In-memory representation of the transactional dataset."""

    customers: Dict[str, Customer]
    categories: Dict[str, Category]
    products: Dict[str, Product]
    orders: Dict[str, Order]
    events: List[Event]

    def customer_ids(self) -> Iterable[str]:
        """Return an iterable over customer identifiers."""

        return self.customers.keys()

    def product_ids(self) -> Iterable[str]:
        """Return an iterable over product identifiers."""

        return self.products.keys()


SQL_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def _parse_date(value: str) -> date:
    """Parse a YYYY-MM-DD string into a :class:`date`."""

    return datetime.strptime(value, "%Y-%m-%d").date()


def _parse_timestamp(value: str) -> datetime:
    """Parse an ISO timestamp into a timezone-naive :class:`datetime`."""

    return datetime.strptime(value, SQL_TIMESTAMP_FORMAT)


def load_dataset() -> Dataset:
    """Load the toy dataset described in the project README.

    The function mirrors the SQL seed statements by creating tiny python objects
    that can be consumed by the ETL and graph algorithms without needing a live
    Postgres instance. This keeps the focus on the transformation logic while
    remaining faithful to the original exercise.
    """

    customers = {
        "C1": Customer(id="C1", name="Alice", join_date=_parse_date("2024-01-02")),
        "C2": Customer(id="C2", name="Bob", join_date=_parse_date("2024-02-11")),
        "C3": Customer(id="C3", name="Chloé", join_date=_parse_date("2024-03-05")),
    }

    categories = {
        "CAT1": Category(id="CAT1", name="Electronics"),
        "CAT2": Category(id="CAT2", name="Books"),
    }

    products = {
        "P1": Product(id="P1", name="Wireless Mouse", price=29.99, category_id="CAT1"),
        "P2": Product(id="P2", name="USB-C Hub", price=49.00, category_id="CAT1"),
        "P3": Product(
            id="P3",
            name="Graph Databases Book",
            price=39.00,
            category_id="CAT2",
        ),
        "P4": Product(id="P4", name="Mechanical Keyboard", price=89.00, category_id="CAT1"),
    }

    orders: Dict[str, Order] = {
        "O1": Order(id="O1", customer_id="C1", placed_at=_parse_timestamp("2024-04-01T10:15:00Z")),
        "O2": Order(id="O2", customer_id="C2", placed_at=_parse_timestamp("2024-04-02T12:30:00Z")),
        "O3": Order(id="O3", customer_id="C1", placed_at=_parse_timestamp("2024-04-05T08:05:00Z")),
    }

    orders["O1"].add_item("P1", 1)
    orders["O1"].add_item("P2", 1)
    orders["O2"].add_item("P3", 1)
    orders["O3"].add_item("P4", 1)
    orders["O3"].add_item("P2", 1)

    events = [
        Event(
            id="E1",
            customer_id="C1",
            product_id="P3",
            event_type="view",
            occurred_at=_parse_timestamp("2024-04-01T09:00:00Z"),
        ),
        Event(
            id="E2",
            customer_id="C1",
            product_id="P3",
            event_type="click",
            occurred_at=_parse_timestamp("2024-04-01T09:01:00Z"),
        ),
        Event(
            id="E3",
            customer_id="C3",
            product_id="P1",
            event_type="view",
            occurred_at=_parse_timestamp("2024-04-03T16:20:00Z"),
        ),
        Event(
            id="E4",
            customer_id="C2",
            product_id="P2",
            event_type="view",
            occurred_at=_parse_timestamp("2024-04-03T12:00:00Z"),
        ),
        Event(
            id="E5",
            customer_id="C2",
            product_id="P4",
            event_type="add_to_cart",
            occurred_at=_parse_timestamp("2024-04-03T12:10:00Z"),
        ),
    ]

    return Dataset(
        customers=customers,
        categories=categories,
        products=products,
        orders=orders,
        events=events,
    )
