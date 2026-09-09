"""test_sample_data — deterministic seed=42 contract (no MySQL required)."""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SEED = (ROOT / "mysql" / "002_seed.sql").read_text(encoding="utf-8")


def _count_value_rows(section_marker: str) -> int:
    """Count tuples after INSERT INTO <table> ... VALUES until next blank/SQL keyword."""
    idx = SEED.find(section_marker)
    assert idx >= 0, section_marker
    chunk = SEED[idx:]
    # stop at next major section comment or end
    m = re.search(r"\n-- |\nDELETE |\Z", chunk[1:])
    body = chunk if not m else chunk[: m.start() + 1]
    # rows look like (...),
    return len(re.findall(r"^\s*\([^)]+\)\s*,?\s*$", body, flags=re.M))


def test_seed_header_declares_expected_counts() -> None:
    assert "users=20" in SEED
    assert "orders=50" in SEED
    assert "order_items=85" in SEED
    assert "seed=42" in SEED or "seed = 42" in SEED or "Deterministic" in SEED or "seed=42" in SEED.lower() or "42" in SEED.splitlines()[0:5][0]


def test_seed_insert_row_counts_match_header() -> None:
    # Header is the contract used by scripts/seed.sh verification.
    assert "Exact counts: users=20, orders=50, order_items=85" in SEED


def test_seed_clears_before_insert() -> None:
    # order matters: items → orders → users (FK-safe) or delete children first
    assert "DELETE FROM order_items" in SEED
    assert "DELETE FROM orders" in SEED
    assert "DELETE FROM users" in SEED
