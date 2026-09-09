#!/usr/bin/env python3
"""ChangeLake Phase 6 / G7 — canonical content fingerprints (SHA256).

Fingerprint is content-only: canonical sort + stable serialization.
It does NOT depend on filenames, physical layout, MinIO keys, or snapshot ids.

Usage:
  # Hash already-canonical TSV lines from stdin (one row per line):
  python3 python/fingerprint.py --stdin

  # Build DWD fingerprint lines from a Flink tableau / TSV dump:
  python3 python/fingerprint.py --role dwd --dt 2026-08-13 < dump.txt

  # Build ADS fingerprint lines:
  python3 python/fingerprint.py --role ads --dt 2026-08-13 < dump.txt

  # Combine two hex digests into a single G7 fingerprint:
  python3 python/fingerprint.py --combine <dwd_sha256> <ads_sha256>
"""
from __future__ import annotations

import argparse
import hashlib
import re
import sys


def sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def canonicalize_lines(lines: list[str]) -> str:
    cleaned = []
    for raw in lines:
        line = raw.rstrip("\r\n")
        if not line.strip():
            continue
        cleaned.append(line)
    cleaned.sort()
    # Trailing newline so empty set still has a defined encoding
    return "\n".join(cleaned) + ("\n" if cleaned else "")


def parse_tableau_or_tsv(text: str, min_cols: int) -> list[list[str]]:
    """Parse Flink SQL-client tableau OR plain TSV into cell lists."""
    rows: list[list[str]] = []
    for line in text.splitlines():
        s = line.strip()
        if not s:
            continue
        if set(s) <= set("+-| "):
            continue
        low = s.lower()
        if "row" in low and "set" in low:
            continue
        if "|" in line:
            cells = [x.strip() for x in line.split("|")]
            # drop empty edge cells from leading/trailing |
            if cells and cells[0] == "":
                cells = cells[1:]
            if cells and cells[-1] == "":
                cells = cells[:-1]
            cells = [c.strip() for c in cells]
        else:
            cells = [c.strip() for c in line.split("\t")]
        if len(cells) < min_cols:
            continue
        # skip header-ish rows
        joined = " ".join(cells).lower()
        if "order_id" in joined or "channel" in joined and "order_cnt" in joined:
            continue
        if "dt" in cells[0].lower() and len(cells) > 1 and "channel" in cells[1].lower():
            continue
        rows.append(cells)
    return rows


def norm_decimal(v: str) -> str:
    v = v.strip()
    if v in ("", "null", "NULL", "<NULL>", "<null>"):
        return "NULL"
    try:
        return f"{float(v):.2f}"
    except ValueError:
        return v


def norm_str(v: str) -> str:
    v = v.strip()
    if v in ("", "null", "NULL", "<NULL>", "<null>"):
        return "NULL"
    return v


def dwd_canonical_lines(text: str, dt: str) -> list[str]:
    """Expect columns: order_id, user_id, status, amount, channel, coupon, net, order_ts, updated_at
    or a subset starting with order_id ... amount ... net_amount.
    We emit: order_id|user_id|status|amount|channel|net_amount
    """
    rows = parse_tableau_or_tsv(text, min_cols=4)
    out: list[str] = []
    for cells in rows:
        # Flexible: if first cell is not int-like, skip
        if not re.fullmatch(r"-?\d+", cells[0]):
            continue
        order_id = cells[0]
        user_id = cells[1] if len(cells) > 1 else "?"
        status = norm_str(cells[2]) if len(cells) > 2 else "?"
        amount = norm_decimal(cells[3]) if len(cells) > 3 else "?"
        channel = norm_str(cells[4]) if len(cells) > 4 else "NULL"
        # Prefer explicit net_amount column if present (index 6 in full dump)
        if len(cells) >= 7:
            net = norm_decimal(cells[6])
        elif len(cells) >= 6:
            # amount, channel, net
            net = norm_decimal(cells[5])
        else:
            net = amount
        out.append(f"{order_id}|{user_id}|{status}|{amount}|{channel}|{net}")
    return out


def ads_canonical_lines(text: str, dt: str) -> list[str]:
    """Expect: channel, order_cnt, paid_order_cnt, gmv, net_gmv, buyer_cnt
    or dt, channel, ...
    Emit: channel|order_cnt|paid_order_cnt|gmv|net_gmv|buyer_cnt
    """
    rows = parse_tableau_or_tsv(text, min_cols=5)
    out: list[str] = []
    for cells in rows:
        # Detect optional leading dt column
        start = 0
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", cells[0]):
            if cells[0] != dt:
                continue
            start = 1
        if len(cells) - start < 6:
            # allow 5 metric cols after channel
            if len(cells) - start < 5:
                continue
        ch = norm_str(cells[start])
        rest = cells[start + 1 :]
        # channel must not look like a pure number-only header leftover
        if ch in ("channel", "CHANNEL"):
            continue
        try:
            oc = str(int(float(rest[0])))
            poc = str(int(float(rest[1])))
            gmv = norm_decimal(rest[2])
            ngmv = norm_decimal(rest[3])
            bc = str(int(float(rest[4])))
        except (ValueError, IndexError):
            continue
        out.append(f"{ch}|{oc}|{poc}|{gmv}|{ngmv}|{bc}")
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--stdin", action="store_true", help="SHA256 of sorted stdin lines")
    ap.add_argument("--role", choices=("dwd", "ads"), help="Parse tableau/TSV for role")
    ap.add_argument("--dt", default="", help="Business date YYYY-MM-DD (ads filter)")
    ap.add_argument("--combine", nargs=2, metavar=("DWD_SHA", "ADS_SHA"))
    ap.add_argument("--emit-lines", action="store_true", help="Print canonical lines instead of hash")
    args = ap.parse_args()

    if args.combine:
        payload = f"dwd={args.combine[0]}\nads={args.combine[1]}\n"
        print(sha256_hex(payload))
        return 0

    text = sys.stdin.read()

    if args.stdin:
        digest = sha256_hex(canonicalize_lines(text.splitlines()))
        print(digest)
        return 0

    if args.role == "dwd":
        lines = dwd_canonical_lines(text, args.dt)
    elif args.role == "ads":
        if not args.dt:
            print("ERROR: --dt required for ads role", file=sys.stderr)
            return 2
        lines = ads_canonical_lines(text, args.dt)
    else:
        ap.print_help()
        return 2

    body = canonicalize_lines(lines)
    if args.emit_lines:
        sys.stdout.write(body)
        return 0
    print(sha256_hex(body))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
