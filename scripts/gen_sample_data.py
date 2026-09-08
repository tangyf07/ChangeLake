#!/usr/bin/env python3
"""Deterministic ChangeLake sample data generator (seed=42).

Writes mysql/002_seed.sql with exact counts:
  users=20, orders=50, order_items=85

Amounts use Decimal only. Statuses:
  created, paid, shipped, completed, cancelled, refunded
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

SEED = 42
CITIES = [
    "Shanghai",
    "Beijing",
    "Shenzhen",
    "Hangzhou",
    "Guangzhou",
    "Chengdu",
    "Nanjing",
    "Wuhan",
    "XiAn",
    "Suzhou",
]
STATUSES = ["created", "paid", "shipped", "completed", "cancelled", "refunded"]
PRODUCTS = {i: Decimal(f"{(10 + (i % 40) * 2.5):.2f}") for i in range(1, 31)}
BASE_TS = datetime(2026, 8, 1, 10, 0, 0)


def fmt_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def generate() -> tuple[list[dict], list[dict], list[dict]]:
    users: list[dict] = []
    for uid in range(1, 21):
        created = BASE_TS + timedelta(days=uid - 1, hours=uid % 5)
        users.append(
            {
                "user_id": uid,
                "username": f"user_{uid:03d}",
                "city": CITIES[(uid + SEED) % len(CITIES)],
                "created_at": created,
                "updated_at": created + timedelta(hours=(uid % 7)),
            }
        )

    orders: list[dict] = []
    for oid in range(1, 51):
        order_ts = BASE_TS + timedelta(
            days=(oid % 28), hours=(oid % 23), minutes=(oid * 3) % 60
        )
        orders.append(
            {
                "order_id": oid,
                "user_id": ((oid * 7 + SEED) % 20) + 1,
                "status": STATUSES[(oid + SEED) % len(STATUSES)],
                "order_ts": order_ts,
                "updated_at": order_ts + timedelta(hours=(oid % 5)),
                "amount": Decimal("0.00"),
            }
        )

    items: list[dict] = []
    item_id = 1
    for o in orders:
        n_items = 1 + ((o["order_id"] * 3 + SEED) % 3)
        if o["order_id"] % 2 == 0 and n_items < 3:
            n_items += 1
        if o["order_id"] % 5 == 0 and n_items < 3:
            n_items += 1
        total = Decimal("0.00")
        for k in range(n_items):
            product_id = ((o["order_id"] * 5 + k * 11 + SEED) % 30) + 1
            qty = 1 + ((o["order_id"] + k) % 3)
            unit_price = PRODUCTS[product_id]
            total += (unit_price * qty).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
            items.append(
                {
                    "item_id": item_id,
                    "order_id": o["order_id"],
                    "product_id": product_id,
                    "qty": qty,
                    "unit_price": unit_price,
                    "updated_at": o["updated_at"],
                }
            )
            item_id += 1
        o["amount"] = total.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    assert len(users) == 20
    assert len(orders) == 50
    assert len(items) >= 80
    return users, orders, items


def render_sql(
    users: list[dict], orders: list[dict], items: list[dict]
) -> str:
    lines: list[str] = [
        "-- ChangeLake Phase 1 seed data",
        f"-- Generated with seed={SEED} via scripts/gen_sample_data.py",
        f"-- Exact counts: users={len(users)}, orders={len(orders)}, order_items={len(items)}",
        "USE changelake;",
        "SET NAMES utf8mb4;",
        "",
        "DELETE FROM order_items;",
        "DELETE FROM orders;",
        "DELETE FROM users;",
        "",
        "-- users",
        "INSERT INTO users (user_id, username, city, created_at, updated_at) VALUES",
    ]
    lines.append(
        ",\n".join(
            f"  ({u['user_id']}, '{u['username']}', '{u['city']}', "
            f"'{fmt_dt(u['created_at'])}', '{fmt_dt(u['updated_at'])}')"
            for u in users
        )
        + ";"
    )
    lines += [
        "",
        "-- orders",
        "INSERT INTO orders (order_id, user_id, status, amount, order_ts, updated_at) VALUES",
    ]
    lines.append(
        ",\n".join(
            f"  ({o['order_id']}, {o['user_id']}, '{o['status']}', {o['amount']}, "
            f"'{fmt_dt(o['order_ts'])}', '{fmt_dt(o['updated_at'])}')"
            for o in orders
        )
        + ";"
    )
    lines += [
        "",
        "-- order_items",
        "INSERT INTO order_items (item_id, order_id, product_id, qty, unit_price, updated_at) VALUES",
    ]
    lines.append(
        ",\n".join(
            f"  ({it['item_id']}, {it['order_id']}, {it['product_id']}, "
            f"{it['qty']}, {it['unit_price']}, '{fmt_dt(it['updated_at'])}')"
            for it in items
        )
        + ";"
    )
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    users, orders, items = generate()
    root = Path(__file__).resolve().parents[1]
    out = root / "mysql" / "002_seed.sql"
    out.write_text(render_sql(users, orders, items), encoding="utf-8")
    dist = Counter(it["order_id"] for it in items)
    print(
        f"wrote {out} "
        f"users={len(users)} orders={len(orders)} order_items={len(items)} "
        f"items/order avg={sum(dist.values())/len(dist):.2f}"
    )


if __name__ == "__main__":
    main()
