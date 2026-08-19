"""Unit tests for the CLI's 'module:attribute' reference loader."""

import pytest
import typer

from cairndb.cli import _load_ref
from cairndb.core.types import SequenceNumber


def test_load_ref_returns_the_attribute():
    assert _load_ref("cairndb.core.types:SequenceNumber") is SequenceNumber


def test_load_ref_rejects_missing_colon():
    with pytest.raises(typer.BadParameter, match="module:attribute"):
        _load_ref("just.a.module.path")


def test_load_ref_splits_on_the_first_colon():
    # The right-hand side is the attribute verbatim — a stray colon there
    # must surface as a missing attribute, not re-split the module path.
    with pytest.raises(typer.BadParameter, match="has no attribute"):
        _load_ref("cairndb.core.types:SequenceNumber:extra")


def test_load_ref_rejects_missing_attribute():
    with pytest.raises(typer.BadParameter, match="has no attribute"):
        _load_ref("cairndb.core.types:Nope")
