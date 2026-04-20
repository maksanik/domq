from unittest.mock import AsyncMock

import pytest

from scripts.analytics import (
    HOT_DEAL_THRESHOLD,
    compute_deal_analysis,
    compute_liquidity_stats,
    compute_price_history,
    compute_price_stats,
)


def test_hot_deal_threshold_constant():
    assert HOT_DEAL_THRESHOLD == 10.0


@pytest.fixture
def conn():
    c = AsyncMock()
    c.execute.return_value = None
    c.fetchval.return_value = 10
    return c


# --- compute_price_stats ---


async def test_compute_price_stats_calls_execute_once(conn):
    await compute_price_stats(conn)
    conn.execute.assert_called_once()


async def test_compute_price_stats_calls_fetchval(conn):
    await compute_price_stats(conn)
    conn.fetchval.assert_called_once()


# --- compute_deal_analysis ---


async def test_compute_deal_analysis_calls_execute_once(conn):
    await compute_deal_analysis(conn)
    conn.execute.assert_called_once()


async def test_compute_deal_analysis_default_threshold_passed(conn):
    await compute_deal_analysis(conn)
    call_args = conn.execute.call_args
    assert HOT_DEAL_THRESHOLD in call_args.args


async def test_compute_deal_analysis_custom_threshold_passed(conn):
    await compute_deal_analysis(conn, threshold=5.0)
    call_args = conn.execute.call_args
    assert 5.0 in call_args.args


async def test_compute_deal_analysis_calls_fetchval_twice(conn):
    await compute_deal_analysis(conn)
    assert conn.fetchval.call_count == 2


# --- compute_price_history ---


async def test_compute_price_history_calls_execute_once(conn):
    await compute_price_history(conn)
    conn.execute.assert_called_once()


async def test_compute_price_history_does_not_call_fetchval(conn):
    await compute_price_history(conn)
    conn.fetchval.assert_not_called()


# --- compute_liquidity_stats ---


async def test_compute_liquidity_stats_calls_execute_once(conn):
    await compute_liquidity_stats(conn)
    conn.execute.assert_called_once()


async def test_compute_liquidity_stats_calls_fetchval(conn):
    await compute_liquidity_stats(conn)
    conn.fetchval.assert_called_once()
