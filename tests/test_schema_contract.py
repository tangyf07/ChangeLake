"""test_schema_contract — MySQL / Paimon money + PK contracts (static)."""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MYSQL = (ROOT / "mysql" / "001_schema.sql").read_text(encoding="utf-8")
ODS = (ROOT / "flink" / "sql" / "ods.sql").read_text(encoding="utf-8")
DWD = (ROOT / "flink" / "sql" / "dwd.sql").read_text(encoding="utf-8")
SEM = (ROOT / "docs" / "semantics.md").read_text(encoding="utf-8")


def test_mysql_money_is_decimal_12_2() -> None:
    compact = MYSQL.replace(" ", "").replace("\n", "")
    assert "amountDECIMAL(12,2)" in compact
    assert "unit_priceDECIMAL(12,2)" in compact
    assert "FLOAT" not in MYSQL.upper()
    assert "DOUBLE" not in MYSQL.upper()


def test_mysql_core_tables_exist() -> None:
    for t in ("CREATE TABLE IF NOT EXISTS users", "CREATE TABLE IF NOT EXISTS orders", "CREATE TABLE IF NOT EXISTS order_items"):
        assert t in MYSQL


def test_ods_uses_paimon_pk_deduplicate() -> None:
    assert "merge-engine" in ODS and "deduplicate" in ODS
    assert "PRIMARY KEY" in ODS


def test_semantics_forbid_float_money() -> None:
    assert "DECIMAL(12,2)" in SEM
    assert "FLOAT" in SEM and "DOUBLE" in SEM  # mentioned as forbidden
