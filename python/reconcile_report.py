#!/usr/bin/env python3
"""ChangeLake Phase 8 / G9 — DECIMAL-safe reconcile report formatter.

Reads JSONL metric rows from stdin (one object per line):
  {"metric":"...","source":"...","lake":"...","kind":"count"|"amount"}

Compares with decimal.Decimal only (never float for money).
Count metrics: exact equality.
Amount metrics: abs(diff) <= tolerance (default 0.01) → PASS else FAIL.

Writes:
  - human table to stdout
  - CSV + JSON report files (--csv / --json)
  - exit 0 if all PASS, exit 2 if any FAIL or parse error

Usage:
  python3 python/reconcile_report.py --csv out.csv --json out.json --tolerance 0.01 < metrics.jsonl
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any


TWOPLACES = Decimal("0.01")


def d(v: Any) -> Decimal:
    """Parse a value as Decimal. Rejects float inputs for honesty."""
    if isinstance(v, float):
        raise TypeError(f"float not allowed for DECIMAL reconcile: {v!r}")
    if v is None:
        raise ValueError("NULL numeric value")
    s = str(v).strip()
    if s in ("", "null", "NULL", "<NULL>", "<null>", "None"):
        raise ValueError(f"empty/null numeric: {v!r}")
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError) as e:
        raise ValueError(f"not a DECIMAL: {v!r}") from e


def quantize_amount(x: Decimal) -> Decimal:
    return x.quantize(TWOPLACES, rounding=ROUND_HALF_UP)


def status_for(kind: str, source: Decimal, lake: Decimal, tol: Decimal) -> tuple[Decimal, str]:
    diff = lake - source
    if kind == "count":
        ok = source == lake
        # counts stay integral
        return diff, ("PASS" if ok else "FAIL")
    # amount
    diff_q = quantize_amount(diff)
    ok = abs(diff) <= tol
    return diff_q, ("PASS" if ok else "FAIL")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--csv", required=True, help="Output CSV path")
    ap.add_argument("--json", required=True, help="Output JSON path")
    ap.add_argument(
        "--tolerance",
        default="0.01",
        help="Amount abs(diff) tolerance as DECIMAL string (default 0.01)",
    )
    ap.add_argument(
        "--title",
        default="source_reconcile_report",
        help="Report title / JSON root name",
    )
    args = ap.parse_args()

    try:
        tol = Decimal(args.tolerance)
    except InvalidOperation:
        print(f"ERROR: bad --tolerance {args.tolerance!r}", file=sys.stderr)
        return 2

    rows_in: list[dict[str, Any]] = []
    for lineno, line in enumerate(sys.stdin, 1):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as e:
            print(f"ERROR: JSONL line {lineno}: {e}", file=sys.stderr)
            return 2
        rows_in.append(obj)

    if not rows_in:
        print("ERROR: no metric rows on stdin", file=sys.stderr)
        return 2

    report_rows: list[dict[str, str]] = []
    any_fail = False

    for obj in rows_in:
        metric = str(obj.get("metric", "")).strip()
        kind = str(obj.get("kind", "amount")).strip().lower()
        if kind not in ("count", "amount"):
            print(f"ERROR: bad kind for {metric}: {kind}", file=sys.stderr)
            return 2
        try:
            source = d(obj.get("source"))
            lake = d(obj.get("lake"))
        except (TypeError, ValueError) as e:
            print(f"ERROR: metric={metric}: {e}", file=sys.stderr)
            return 2
        if kind == "amount":
            source = quantize_amount(source)
            lake = quantize_amount(lake)
        else:
            # normalize count to integer Decimal
            if source != source.to_integral_value() or lake != lake.to_integral_value():
                print(f"ERROR: count metric not integral: {metric}", file=sys.stderr)
                return 2
            source = source.to_integral_value()
            lake = lake.to_integral_value()

        diff, status = status_for(kind, source, lake, tol)
        if status == "FAIL":
            any_fail = True
        report_rows.append(
            {
                "metric": metric,
                "source": format(source, "f"),
                "lake": format(lake, "f"),
                "diff": format(diff, "f"),
                "status": status,
                "kind": kind,
            }
        )

    # Human table
    headers = ("metric", "source", "lake", "diff", "status")
    widths = {h: len(h) for h in headers}
    for r in report_rows:
        for h in headers:
            widths[h] = max(widths[h], len(r[h]))

    def fmt_row(r: dict[str, str]) -> str:
        return "  ".join(r[h].ljust(widths[h]) for h in headers)

    print(fmt_row({h: h for h in headers}))
    print(fmt_row({h: "-" * widths[h] for h in headers}))
    for r in report_rows:
        print(fmt_row(r))

    fail_n = sum(1 for r in report_rows if r["status"] == "FAIL")
    pass_n = sum(1 for r in report_rows if r["status"] == "PASS")
    print()
    print(f"summary  pass={pass_n}  fail={fail_n}  tolerance={tol}  decimal_only=yes")

    # CSV
    with open(args.csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(headers))
        w.writeheader()
        for r in report_rows:
            w.writerow({h: r[h] for h in headers})

    # JSON
    payload = {
        "report": args.title,
        "tolerance": format(tol, "f"),
        "decimal_only": True,
        "pass_count": pass_n,
        "fail_count": fail_n,
        "overall": "FAIL" if any_fail else "PASS",
        "rows": [{h: r[h] for h in headers} for r in report_rows],
        "notes": [
            "Current-state MySQL ↔ lake reconcile (demo check).",
            "Amount comparisons use decimal.Decimal; abs(diff) <= tolerance.",
            "ODS amount-by-channel keeps NULL as NULL (ADS 'unknown' is separate).",
            "NOT claimed: continuous monitoring / EO-2PC / Exactly-Once E2E.",
        ],
    }
    with open(args.json, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
        f.write("\n")

    return 2 if any_fail else 0


if __name__ == "__main__":
    raise SystemExit(main())
