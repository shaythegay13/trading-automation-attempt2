"""Locks the read-only contract: public GETs only, live trading off by default."""

import ast
from pathlib import Path

import pytest

from src.config import Settings
from src.runner import _refuse_live_trading

CLIENT_SOURCES = [
    Path("src/clients/base.py"),
    Path("src/clients/kalshi.py"),
    Path("src/clients/polymarket.py"),
    Path("src/runner.py"),
]

# HTTP methods that would mutate an account or a book.
FORBIDDEN_HTTP_METHODS = {"post", "put", "patch", "delete"}
FORBIDDEN_DEF_NAMES = {
    "place_order",
    "create_order",
    "submit_order",
    "cancel_order",
    "amend_order",
}


def test_live_trading_defaults_off():
    assert Settings().live_trading_enabled is False


def test_live_trading_env_must_be_explicit(monkeypatch):
    monkeypatch.setenv("LIVE_TRADING_ENABLED", "false")
    assert Settings().live_trading_enabled is False
    monkeypatch.setenv("LIVE_TRADING_ENABLED", "true")
    assert Settings().live_trading_enabled is True


def test_runner_refuses_to_start_when_live_trading_enabled(monkeypatch):
    monkeypatch.setattr("src.runner.settings.live_trading_enabled", True)
    with pytest.raises(RuntimeError, match="no order-placement"):
        _refuse_live_trading()


def test_runner_allows_read_only_startup(monkeypatch):
    monkeypatch.setattr("src.runner.settings.live_trading_enabled", False)
    _refuse_live_trading()


def test_ingestion_sources_do_not_send_orders():
    for path in CLIENT_SOURCES:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                assert node.func.attr not in FORBIDDEN_HTTP_METHODS, (
                    f"{path}:{node.lineno} calls .{node.func.attr}()"
                )
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                assert node.name not in FORBIDDEN_DEF_NAMES, (
                    f"{path}:{node.lineno} defines {node.name}"
                )
