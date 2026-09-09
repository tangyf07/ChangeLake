"""test_backfill_idempotency — G7 fingerprint stability (content-only)."""
from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "fingerprint", ROOT / "python" / "fingerprint.py"
)
assert spec and spec.loader
fp = importlib.util.module_from_spec(spec)
sys.modules["fingerprint"] = fp
spec.loader.exec_module(fp)


def test_same_canonical_rows_same_hash() -> None:
    rows = [
        "1|1|paid|10.00|app|10.00",
        "2|1|open|5.50|web|5.50",
    ]
    a = fp.sha256_hex(fp.canonicalize_lines(rows))
    b = fp.sha256_hex(fp.canonicalize_lines(list(reversed(rows))))
    assert a == b


def test_combine_dwd_ads_is_deterministic() -> None:
    dwd = "a" * 64
    ads = "b" * 64
    proc1 = subprocess.run(
        [sys.executable, str(ROOT / "python" / "fingerprint.py"), "--combine", dwd, ads],
        capture_output=True,
        text=True,
        check=True,
    )
    proc2 = subprocess.run(
        [sys.executable, str(ROOT / "python" / "fingerprint.py"), "--combine", dwd, ads],
        capture_output=True,
        text=True,
        check=True,
    )
    assert proc1.stdout.strip() == proc2.stdout.strip()
    assert len(proc1.stdout.strip()) == 64


def test_order_independent_combine_payload_differs_when_parts_swap() -> None:
    # Documented contract: combine is ordered dwd then ads (not commutative).
    h1 = subprocess.check_output(
        [sys.executable, str(ROOT / "python" / "fingerprint.py"), "--combine", "aa", "bb"],
        text=True,
    ).strip()
    h2 = subprocess.check_output(
        [sys.executable, str(ROOT / "python" / "fingerprint.py"), "--combine", "bb", "aa"],
        text=True,
    ).strip()
    assert h1 != h2
