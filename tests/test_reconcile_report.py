"""Unit tests for python/reconcile_report.py DECIMAL-safe compare."""
from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "reconcile_report", ROOT / "python" / "reconcile_report.py"
)
assert spec and spec.loader
rr = importlib.util.module_from_spec(spec)
sys.modules["reconcile_report"] = rr
spec.loader.exec_module(rr)


def test_count_exact() -> None:
    diff, st = rr.status_for("count", Decimal("10"), Decimal("10"), Decimal("0.01"))
    assert st == "PASS" and diff == Decimal("0")
    _, st2 = rr.status_for("count", Decimal("10"), Decimal("11"), Decimal("0.01"))
    assert st2 == "FAIL"


def test_amount_tolerance() -> None:
    _, st = rr.status_for("amount", Decimal("100.00"), Decimal("100.01"), Decimal("0.01"))
    assert st == "PASS"
    _, st2 = rr.status_for("amount", Decimal("100.00"), Decimal("100.02"), Decimal("0.01"))
    assert st2 == "FAIL"


def test_rejects_float() -> None:
    try:
        rr.d(1.23)  # type: ignore[arg-type]
        assert False, "expected TypeError"
    except TypeError:
        pass


def test_cli_pass(tmp_path: Path) -> None:
    csv_p = tmp_path / "r.csv"
    json_p = tmp_path / "r.json"
    payload = (
        '{"metric":"orders","source":"10","lake":"10","kind":"count"}\n'
        '{"metric":"gmv","source":"100.00","lake":"100.01","kind":"amount"}\n'
    )
    proc = subprocess.run(
        [
            sys.executable,
            str(ROOT / "python" / "reconcile_report.py"),
            "--csv",
            str(csv_p),
            "--json",
            str(json_p),
            "--tolerance",
            "0.01",
        ],
        input=payload,
        text=True,
        capture_output=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    data = json.loads(json_p.read_text(encoding="utf-8"))
    assert data["overall"] == "PASS"
    assert data["fail_count"] == 0
    assert "PASS" in csv_p.read_text(encoding="utf-8")
