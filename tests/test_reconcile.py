"""test_reconcile — DECIMAL-safe MySQL↔lake compare helpers (G9)."""
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


def test_count_metric_exact_only() -> None:
    _, st = rr.status_for("count", Decimal("68"), Decimal("68"), Decimal("0.01"))
    assert st == "PASS"
    _, st2 = rr.status_for("count", Decimal("68"), Decimal("67"), Decimal("0.01"))
    assert st2 == "FAIL"


def test_amount_metric_uses_tolerance_not_float() -> None:
    _, st = rr.status_for("amount", Decimal("100.00"), Decimal("100.01"), Decimal("0.01"))
    assert st == "PASS"
    try:
        rr.d(100.01)  # type: ignore[arg-type]
        raise AssertionError("float must be rejected")
    except TypeError:
        pass


def test_cli_overall_pass(tmp_path: Path) -> None:
    csv_p, json_p = tmp_path / "a.csv", tmp_path / "a.json"
    payload = '{"metric":"orders.cnt","source":"50","lake":"50","kind":"count"}\n'
    proc = subprocess.run(
        [
            sys.executable,
            str(ROOT / "python" / "reconcile_report.py"),
            "--csv",
            str(csv_p),
            "--json",
            str(json_p),
        ],
        input=payload,
        text=True,
        capture_output=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    assert json.loads(json_p.read_text(encoding="utf-8"))["overall"] == "PASS"
