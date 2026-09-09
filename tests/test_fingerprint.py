"""Unit tests for python/fingerprint.py (content-only SHA256 helpers)."""
from __future__ import annotations

import importlib.util
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


def test_canonicalize_sorts_and_trailing_newline() -> None:
    body = fp.canonicalize_lines(["b\n", "a\n", "a\n"])
    assert body == "a\na\nb\n"
    assert fp.sha256_hex(body) == fp.sha256_hex("a\na\nb\n")


def test_canonicalize_empty() -> None:
    assert fp.canonicalize_lines([]) == ""
    assert fp.sha256_hex("") == (
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    )


def test_norm_decimal() -> None:
    assert fp.norm_decimal("1.1") == "1.10"
    assert fp.norm_decimal("NULL") == "NULL"


def test_dwd_canonical_lines_from_tsv() -> None:
    text = "101\t7\topen\t12.5\tweb\t0\t12.50\n"
    lines = fp.dwd_canonical_lines(text, "2026-08-13")
    assert lines == ["101|7|open|12.50|web|12.50"]
    digest = fp.sha256_hex(fp.canonicalize_lines(lines))
    assert len(digest) == 64
