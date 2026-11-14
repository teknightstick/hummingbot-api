"""Issue #222 integration gate for an externally supplied real Hummingbot build.

Set ``ISSUE222_HUMMINGBOT_ROOT`` to the exact accepted Hummingbot source/build root
that contains the importable ``hummingbot`` package.  Absence is an explicit B9 gap,
not a dependency-light substitute.
"""

import asyncio
import importlib
import os
import sys
import unittest
from decimal import Decimal
from pathlib import Path

import pytest


CHECK = unittest.TestCase()
ROOT_ENV = "ISSUE222_HUMMINGBOT_ROOT"


def _load_real_core():
    supplied = os.environ.get(ROOT_ENV)
    if not supplied:
        pytest.skip(
            f"B9 GAP: {ROOT_ENV} is absent; real Hummingbot integration did not run"
        )

    root = Path(supplied).expanduser().resolve()
    if not root.is_dir() or not (root / "hummingbot").is_dir():
        pytest.fail(
            f"B9 INVALID ROOT: {ROOT_ENV} must contain an importable hummingbot package"
        )

    root_text = str(root)
    if root_text not in sys.path:
        sys.path.insert(0, root_text)
    importlib.invalidate_caches()
    try:
        order_book = importlib.import_module(
            "hummingbot.core.data_type.order_book"
        )
        order_book_row = importlib.import_module(
            "hummingbot.core.data_type.order_book_row"
        )
        tracker = importlib.import_module(
            "hummingbot.core.data_type.order_book_tracker"
        )
        unified = importlib.import_module("services.unified_connector_service")
    except Exception as exc:
        pytest.fail(
            "B9 REAL CORE IMPORT FAILED: "
            f"{type(exc).__name__}; build the supplied root before retrying"
        )
    for module in (order_book, order_book_row, tracker):
        module_file = getattr(module, "__file__", None)
        if module_file is None or not Path(module_file).resolve().is_relative_to(root):
            pytest.fail(
                "B9 WRONG CORE: imported Hummingbot module is outside the "
                f"exact {ROOT_ENV} root"
            )
    return order_book, order_book_row, tracker, unified


def test_real_core_query_primitives():
    order_book_module, row_module, _, _ = _load_real_core()
    book = order_book_module.OrderBook()
    row = row_module.OrderBookRow
    book.apply_snapshot(
        [row(Decimal("99"), Decimal("2"), 1)],
        [row(Decimal("101"), Decimal("2"), 1)],
        1,
    )

    cases = (
        (book.get_price_for_volume(True, 1), Decimal("101"), Decimal("1")),
        (book.get_price_for_volume(False, 1), Decimal("99"), Decimal("1")),
        (book.get_price_for_quote_volume(True, 101), Decimal("101"), Decimal("101")),
        (book.get_volume_for_price(True, 101), Decimal("101"), Decimal("2")),
        (book.get_quote_volume_for_price(False, 99), Decimal("99"), Decimal("198")),
        (book.get_vwap_for_volume(True, 1), Decimal("101"), Decimal("1")),
    )
    for result, expected_price, expected_volume in cases:
        CHECK.assertEqual(expected_price, Decimal(str(result.result_price)))
        CHECK.assertEqual(expected_volume, Decimal(str(result.result_volume)))


@pytest.mark.asyncio
async def test_real_order_book_tracker_task_inventory_is_collected():
    _, _, tracker_module, unified_module = _load_real_core()
    tracker = tracker_module.OrderBookTracker(None, ["BTC-USD"])
    task_names = (
        "_init_order_books_task",
        "_emit_trade_event_task",
        "_order_book_stream_listener_task",
        "_order_book_diff_listener_task",
        "_order_book_trade_listener_task",
        "_order_book_snapshot_listener_task",
        "_order_book_snapshot_router_task",
        "_order_book_diff_router_task",
        "_update_last_trade_prices_task",
    )

    async def pending():
        await asyncio.Event().wait()

    tasks = [asyncio.create_task(pending()) for _ in range(len(task_names) + 1)]
    await asyncio.sleep(0)
    for name, task in zip(task_names, tasks):
        setattr(tracker, name, task)
    tracker._tracking_tasks["BTC-USD"] = tasks[-1]

    try:
        collected = unified_module.UnifiedConnectorService._collect_tracker_tasks(
            tracker
        )
        CHECK.assertEqual(set(tasks), collected)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
