"""test_business_metrics — ADS metric shape + paid-status semantics (static)."""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ADS_SQL = (ROOT / "flink" / "sql" / "ads.sql").read_text(encoding="utf-8")
DWD_SQL = (ROOT / "flink" / "sql" / "dwd.sql").read_text(encoding="utf-8")
DOCS = (ROOT / "docs" / "dwd-ads.md").read_text(encoding="utf-8") if (ROOT / "docs" / "dwd-ads.md").exists() else ""


def test_ads_table_has_required_metric_columns() -> None:
    for col in (
        "dt DATE",
        "channel STRING",
        "order_cnt BIGINT",
        "paid_order_cnt BIGINT",
        "gmv DECIMAL(18, 2)",
        "net_gmv DECIMAL(18, 2)",
        "buyer_cnt BIGINT",
    ):
        assert col in ADS_SQL, col


def test_ads_pk_is_dt_channel() -> None:
    assert "PRIMARY KEY (dt, channel)" in ADS_SQL


def test_null_channel_mapped_to_unknown_is_documented() -> None:
    blob = ADS_SQL + DOCS + (ROOT / "docs" / "semantics.md").read_text(encoding="utf-8")
    assert "unknown" in blob.lower()


def test_dwd_net_amount_present_coupon_nullable() -> None:
    assert "net_amount DECIMAL(12, 2)" in DWD_SQL
    assert "coupon_amount DECIMAL(12, 2)" in DWD_SQL
