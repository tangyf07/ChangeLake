"""Static config checks — no Docker required (CI-friendly)."""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_env_example_has_demo_credentials_keys() -> None:
    text = (ROOT / ".env.example").read_text(encoding="utf-8")
    for key in (
        "MYSQL_ROOT_PASSWORD",
        "MINIO_ROOT_USER",
        "MINIO_ROOT_PASSWORD",
        "PAIMON_WAREHOUSE",
        "FLINK_UI_PORT",
    ):
        assert key in text, f"missing {key} in .env.example"


def test_compose_lists_core_services() -> None:
    text = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    for svc in ("mysql:", "minio:", "jobmanager:", "taskmanager:"):
        assert svc in text, f"missing service {svc}"


def test_makefile_has_test_and_demo() -> None:
    text = (ROOT / "Makefile").read_text(encoding="utf-8")
    assert "\ntest:" in text or text.startswith("test:") or "\ntest:\n" in text or "test:" in text
    assert "demo:" in text


def test_semantics_and_limitations_exist() -> None:
    assert (ROOT / "docs" / "semantics.md").stat().st_size > 200
    assert (ROOT / "docs" / "limitations.md").stat().st_size > 200
