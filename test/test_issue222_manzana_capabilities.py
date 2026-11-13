"""API-local command semantics for Issue #222.

The production image supplies Hummingbot's compiled package.  The isolated Windows
contract runner used for this issue does not, so a per-test import fixture exposes
only the names needed to import the API service and restores ``sys.modules`` after
each test. Behavioral fakes implement the same pinned OrderBook primitive contracts.
"""

import asyncio
import ast
import importlib
import json
import math
import os
import subprocess
import sys
import types
import unittest
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from pathlib import Path

import pytest
from pydantic import BaseModel


CHECK = unittest.TestCase()


def _module(name: str) -> types.ModuleType:
    module = sys.modules.get(name)
    if module is None:
        module = types.ModuleType(name)
        module.__issue222_contract_shim__ = True
        sys.modules[name] = module
    return module


def _install_import_contract() -> None:
    if importlib.util.find_spec("hummingbot") is not None:
        return

    for package in (
        "hummingbot",
        "hummingbot.client",
        "hummingbot.connector",
        "hummingbot.core",
        "hummingbot.core.data_type",
        "hummingbot.data_feed",
        "hummingbot.data_feed.candles_feed",
    ):
        module = _module(package)
        module.__path__ = []

    services = _module("services")
    services.__path__ = [str(Path(__file__).resolve().parents[1] / "services")]
    models = _module("models")
    models.__path__ = [str(Path(__file__).resolve().parents[1] / "models")]
    routers = _module("routers")
    routers.__path__ = [str(Path(__file__).resolve().parents[1] / "routers")]

    settings_module = _module("hummingbot.client.settings")
    settings_module.AllConnectorSettings = type(
        "AllConnectorSettings",
        (),
        {"get_connector_settings": staticmethod(lambda: {})},
    )

    deps = _module("deps")
    deps.get_accounts_service = lambda: None
    deps.get_connector_service = lambda: None
    deps.get_market_data_service = lambda: None
    deps.get_trading_history_service = lambda: None

    accounts_service = _module("services.accounts_service")
    accounts_service.AccountsService = type("AccountsService", (), {})
    history_service = _module("services.trading_history_service")
    history_service.TradingHistoryService = type("TradingHistoryService", (), {})

    config = _module("config")
    config.settings = types.SimpleNamespace(
        market_data=types.SimpleNamespace(candles_ready_timeout=1.0)
    )

    connector_utils = _module("hummingbot.connector.utils")
    connector_utils.combine_to_hb_trading_pair = lambda base, quote: f"{base}-{quote}"
    connector_utils.split_hb_trading_pair = lambda trading_pair: tuple(trading_pair.rsplit("-", 1))

    connector_base = _module("hummingbot.connector.connector_base")
    connector_base.ConnectorBase = type("ConnectorBase", (), {})

    common = _module("hummingbot.core.data_type.common")

    class OrderType(Enum):
        LIMIT = 1
        MARKET = 2
        LIMIT_MAKER = 3

    class PositionAction(Enum):
        NIL = 0
        OPEN = 1
        CLOSE = 2

    class PositionMode(Enum):
        HEDGE = 1
        ONEWAY = 2

    class TradeType(Enum):
        BUY = 1
        SELL = 2

    common.OrderType = OrderType
    common.PositionAction = PositionAction
    common.PositionMode = PositionMode
    common.TradeType = TradeType

    candles_factory = _module("hummingbot.data_feed.candles_feed.candles_factory")
    candles_factory.CandlesFactory = type("CandlesFactory", (), {"_candles_map": {}})
    candles_factory.UnsupportedConnectorException = type(
        "UnsupportedConnectorException", (Exception,), {}
    )

    candles_types = _module("hummingbot.data_feed.candles_feed.data_types")
    candles_types.CandlesConfig = type("CandlesConfig", (), {})
    class HistoricalCandlesConfig(BaseModel):
        connector_name: str
        trading_pair: str
        interval: str = "1m"

    candles_types.HistoricalCandlesConfig = HistoricalCandlesConfig

    ticker_sources = _module("services.ticker_sources")
    ticker_sources.Ticker = type("Ticker", (), {})
    ticker_sources.TickerFetchError = type("TickerFetchError", (RuntimeError,), {})
    ticker_sources.TickerUnsupportedError = type(
        "TickerUnsupportedError", (ticker_sources.TickerFetchError,), {}
    )

    async def _unused_fetch_tickers(*args, **kwargs):
        raise AssertionError("ticker transport is outside this isolated contract")

    ticker_sources.fetch_tickers = _unused_fetch_tickers

    unified = _module("services.unified_connector_service")
    unified.UnknownConnectorError = type("UnknownConnectorError", (ValueError,), {})

    market_models = importlib.import_module("models.market_data")
    connector_models = importlib.import_module("models.connectors")
    pagination_models = importlib.import_module("models.pagination")
    trading_models = importlib.import_module("models.trading")
    for name in (
        "AddTradingPairRequest",
        "FundingInfoRequest",
        "FundingInfoResponse",
        "OrderBookLevel",
        "OrderBookQueryResult",
        "OrderBookRequest",
        "OrderBookResponse",
        "PoolPricesResponse",
        "PriceForQuoteVolumeRequest",
        "PriceForVolumeRequest",
        "PriceRequest",
        "PricesResponse",
        "QuoteVolumeForPriceRequest",
        "RateRequest",
        "RateNotFoundDetail",
        "RateNotFoundResponse",
        "RatesResponse",
        "RemoveTradingPairRequest",
        "SingleRateResponse",
        "TickerInfo",
        "TickersResponse",
        "TradingPairResponse",
        "VolumeForPriceRequest",
        "VWAPForVolumeRequest",
    ):
        setattr(models, name, getattr(market_models, name))
    models.CapabilityErrorDetail = connector_models.CapabilityErrorDetail
    models.CapabilityErrorResponse = connector_models.CapabilityErrorResponse
    for name in (
        "ActiveOrderFilterRequest",
        "FundingPaymentFilterRequest",
        "OrderFilterRequest",
        "PositionFilterRequest",
        "TradeFilterRequest",
        "TradeRequest",
        "TradeResponse",
    ):
        setattr(models, name, getattr(trading_models, name))
    models.PaginatedResponse = pagination_models.PaginatedResponse


MarketDataService = None
OrderBookQueryResult = None


def _is_contract_module(name: str) -> bool:
    return name in {"deps", "config"} or name.startswith(
        ("hummingbot", "models", "routers", "services", "utils")
    )


@pytest.fixture(autouse=True)
def _scoped_import_contract(request):
    """Install dependency-light shims only while an individual test executes."""

    if request.node.name in {
        "test_b6_capability_json_is_deterministic_across_hash_seeds",
        "test_b7_contract_module_does_not_pollute_imports_or_use_bare_asserts",
    }:
        yield
        return

    before = {
        name: module for name, module in sys.modules.items() if _is_contract_module(name)
    }
    _install_import_contract()
    from models.market_data import OrderBookQueryResult as ResultModel
    from services.market_data_service import MarketDataService as Service

    globals()["MarketDataService"] = Service
    globals()["OrderBookQueryResult"] = ResultModel
    try:
        yield
    finally:
        globals()["MarketDataService"] = None
        globals()["OrderBookQueryResult"] = None
        for name in tuple(sys.modules):
            if _is_contract_module(name) and name not in before:
                sys.modules.pop(name, None)
        for name, module in before.items():
            sys.modules[name] = module


@dataclass
class QueryResult:
    query_price: float
    query_volume: float
    result_price: float
    result_volume: float


class TwoLevelOrderBook:
    """Pinned OrderBook quote primitives over asks [(100, 1), (110, 1)]."""

    asks = ((100.0, 1.0), (110.0, 1.0))

    def get_price_for_quote_volume(self, is_buy: bool, quote_volume: float) -> QueryResult:
        CHECK.assertIs(is_buy, True)
        cumulative = 0.0
        result_price = float("nan")
        for price, amount in self.asks:
            cumulative += price * amount
            if cumulative >= quote_volume:
                result_price = price
                break
        return QueryResult(float("nan"), quote_volume, result_price, min(cumulative, quote_volume))

    def get_quote_volume_for_price(self, is_buy: bool, price: float) -> QueryResult:
        CHECK.assertIs(is_buy, True)
        cumulative = 0.0
        result_price = float("nan")
        for level_price, amount in self.asks:
            if level_price > price:
                break
            cumulative += level_price * amount
            result_price = level_price
        return QueryResult(price, float("nan"), result_price, cumulative)

    def get_vwap_for_volume(self, is_buy: bool, volume: float) -> QueryResult:
        CHECK.assertIs(is_buy, True)
        total_cost = 0.0
        total_volume = 0.0
        for price, amount in self.asks:
            consumed = min(amount, volume - total_volume)
            total_cost += price * consumed
            total_volume += consumed
            if total_volume >= volume:
                break
        return QueryResult(float("nan"), volume, total_cost / total_volume, total_volume)


class OrderBookConnectorService:
    def __init__(self, order_book):
        tracker = types.SimpleNamespace(order_books={"BTC-USD": order_book})
        self.connector = types.SimpleNamespace(order_book_tracker=tracker, _orderbook_ds=None)

    def get_best_connector_for_market(self, connector_name, account_name=None):
        CHECK.assertEqual("static", connector_name)
        return self.connector


@pytest.mark.asyncio
async def test_quote_order_book_queries_use_the_pinned_primitives():
    service = MarketDataService(OrderBookConnectorService(TwoLevelOrderBook()))

    price_for_quote = await service.get_order_book_query_result(
        "static", "BTC-USD", True, quote_volume=150.0
    )
    quote_for_price = await service.get_order_book_query_result(
        "static", "BTC-USD", True, quote_price=110.0
    )

    CHECK.assertEqual(price_for_quote, {
        "trading_pair": "BTC-USD",
        "is_buy": True,
        "query_volume": 150.0,
        "result_price": 110.0,
        "result_quote_volume": 150.0,
        "timestamp": price_for_quote["timestamp"],
    })
    CHECK.assertEqual(quote_for_price, {
        "trading_pair": "BTC-USD",
        "is_buy": True,
        "query_price": 110.0,
        "result_price": 110.0,
        "result_quote_volume": 210.0,
        "timestamp": quote_for_price["timestamp"],
    })


@pytest.mark.asyncio
async def test_vwap_price_is_canonical_in_service_and_openapi_model():
    service = MarketDataService(OrderBookConnectorService(TwoLevelOrderBook()))

    result = await service.get_order_book_query_result(
        "static", "BTC-USD", True, vwap_volume=2.0
    )

    CHECK.assertEqual(105.0, result["result_price"])
    CHECK.assertNotIn("average_price", result)
    schema_properties = OrderBookQueryResult.model_json_schema()["properties"]
    CHECK.assertIn("result_price", schema_properties)
    CHECK.assertNotIn("average_price", schema_properties)
    CHECK.assertIn("result_price", OrderBookQueryResult.model_json_schema()["required"])
    CHECK.assertEqual("number", schema_properties["result_price"]["type"])
    CHECK.assertEqual(105.0, OrderBookQueryResult(**result).result_price)


def test_connector_capability_registry_is_declared_and_noninvasive():
    from models.connectors import ConnectorCapabilities
    from services.connector_capabilities import ConnectorCapabilityRegistry, ConnectorFeature

    registry = ConnectorCapabilityRegistry()
    manzana = registry.get("manzana")

    CHECK.assertIs(manzana.declared, True)
    CHECK.assertEqual(frozenset({"BTC-USD"}), manzana.supported_pairs)
    CHECK.assertIs(manzana.dynamic_subscription, False)
    CHECK.assertIs(manzana.perpetual, False)
    CHECK.assertIs(manzana.candles, False)
    CHECK.assertIs(manzana.last_trade, False)
    CHECK.assertIn(ConnectorFeature.ORDER_BOOK, manzana.features)
    CHECK.assertIs(registry.supports_pair("manzana", "BTC-USD"), True)
    CHECK.assertIs(registry.supports_pair("manzana", "ETH-USD"), False)

    undeclared = registry.get("binance")
    CHECK.assertIs(undeclared.declared, False)
    CHECK.assertIs(registry.supports_pair("binance", "ANY-PAIR"), True)
    CHECK.assertIs(registry.supports_feature("binance", ConnectorFeature.CANDLES), True)

    class ConnectorDeclared:
        api_capabilities = {
            "supported_pairs": {"ONE-USD"},
            "features": {ConnectorFeature.ORDER_BOOK},
            "dynamic_subscription": False,
            "perpetual": False,
            "candles": False,
            "last_trade": False,
        }

    declared = registry.get("custom", ConnectorDeclared())
    CHECK.assertIs(declared.declared, True)
    CHECK.assertEqual(frozenset({"ONE-USD"}), declared.supported_pairs)
    CHECK.assertIs(registry.supports_pair("custom", "ONE-USD", ConnectorDeclared()), True)


@pytest.mark.asyncio
async def test_capabilities_route_exposes_api_local_declaration_without_lookup():
    from routers.connectors import get_connector_capabilities
    from services.connector_capabilities import ConnectorCapabilityRegistry

    class CapabilityConnectorService:
        def __init__(self):
            self.registry = ConnectorCapabilityRegistry()

        def get_connector_capabilities(self, connector_name):
            return self.registry.get(connector_name)

        def is_known_connector(self, connector_name):
            return False

    market_data_service = MarketDataService(CapabilityConnectorService())
    request = types.SimpleNamespace(
        app=types.SimpleNamespace(
            state=types.SimpleNamespace(market_data_service=market_data_service)
        )
    )

    capabilities = await get_connector_capabilities(request, "manzana")

    CHECK.assertIs(capabilities.declared, True)
    CHECK.assertEqual("api-local-pending-connector-seam", capabilities.source)
    CHECK.assertEqual(frozenset({"BTC-USD"}), capabilities.supported_pairs)


@pytest.mark.asyncio
async def test_unsupported_pair_and_feature_reject_before_connector_lookup():
    from models.connectors import CapabilityErrorResponse
    from services.connector_capabilities import (
        ConnectorCapabilityError,
        ConnectorCapabilityRegistry,
        ConnectorFeature,
    )

    class GuardedConnectorService:
        def __init__(self):
            self.registry = ConnectorCapabilityRegistry()
            self.connector_lookups = 0

        def require_capability(self, connector_name, feature, trading_pair=None):
            self.registry.require(connector_name, feature, trading_pair)

        def get_best_connector_for_market(self, connector_name, account_name=None):
            self.connector_lookups += 1
            raise AssertionError("unsupported request reached connector lookup")

    connector_service = GuardedConnectorService()
    service = MarketDataService(connector_service)

    with pytest.raises(ConnectorCapabilityError) as pair_error:
        await service.get_order_book_data("manzana", "ETH-USD")
    CHECK.assertEqual(400, pair_error.value.status_code)
    CHECK.assertEqual(pair_error.value.detail, {
        "code": "unsupported_trading_pair",
        "message": "Connector 'manzana' does not support trading pair 'ETH-USD'",
        "connector": "manzana",
        "feature": ConnectorFeature.ORDER_BOOK.value,
        "trading_pair": "ETH-USD",
        "supported_pairs": ["BTC-USD"],
    })
    CapabilityErrorResponse.model_validate({"detail": pair_error.value.detail})

    with pytest.raises(ConnectorCapabilityError) as feature_error:
        await service.get_prices("manzana", ["BTC-USD"])
    CHECK.assertEqual(400, feature_error.value.status_code)
    CHECK.assertEqual("unsupported_connector_feature", feature_error.value.detail["code"])
    CHECK.assertEqual(ConnectorFeature.LAST_TRADE.value, feature_error.value.detail["feature"])

    with pytest.raises(ConnectorCapabilityError) as readiness_error:
        await service.initialize_order_book("manzana", "BTC-USD")
    CHECK.assertEqual(400, readiness_error.value.status_code)
    CHECK.assertEqual("live_order_book", readiness_error.value.detail["feature"])
    CHECK.assertEqual(0, connector_service.connector_lookups)


@pytest.mark.asyncio
async def test_trading_rules_mixed_is_typed_partial_and_single_missing_fails():
    import json

    from fastapi import HTTPException

    from routers.connectors import get_trading_rules

    valid_rule = {"min_order_size": 0.001}

    class RulesService:
        async def get_trading_rules(self, connector_name, trading_pairs):
            return {
                pair: (
                    valid_rule
                    if pair == "BTC-USD"
                    else {"error": "raw connector KeyError must not escape"}
                )
                for pair in trading_pairs
            }

    request = types.SimpleNamespace(
        app=types.SimpleNamespace(
            state=types.SimpleNamespace(market_data_service=RulesService())
        )
    )

    partial = await get_trading_rules(
        request, "generic", ["BTC-USD", "ETH-USD"]
    )
    CHECK.assertEqual(207, partial.status_code)
    partial_body = json.loads(partial.body)
    CHECK.assertEqual(partial_body, {
        "connector": "generic",
        "rules": {"BTC-USD": valid_rule},
        "errors": {
            "ETH-USD": {
                "code": "trading_pair_not_found",
                "message": "Trading pair 'ETH-USD' is not available",
                "trading_pair": "ETH-USD",
            }
        },
    })
    CHECK.assertNotIn("KeyError", partial.body.decode())

    with pytest.raises(HTTPException) as missing:
        await get_trading_rules(request, "generic", ["ETH-USD"])
    CHECK.assertEqual(404, missing.value.status_code)
    CHECK.assertEqual(missing.value.detail, {
        "code": "trading_rules_not_found",
        "message": "No trading rules found for the requested pairs",
        "connector": "generic",
        "trading_pairs": ["ETH-USD"],
    })


@pytest.mark.asyncio
async def test_connector_scoped_missing_rates_fail_but_unscoped_null_is_preserved():
    from fastapi import HTTPException

    from models.market_data import RateNotFoundResponse, RateRequest
    from routers.market_data import get_rates, get_single_rate

    class RateService:
        quote_token = "USD"

        def get_rate_for_connector(self, connector_name, base, quote=None):
            return None

        def get_pair_rate(self, trading_pair):
            return None

    service = RateService()

    with pytest.raises(HTTPException) as single:
        await get_single_rate("BTC-USD", "manzana", service)
    CHECK.assertEqual(404, single.value.status_code)
    CHECK.assertEqual(single.value.detail, {
        "code": "connector_rate_not_found",
        "message": "Rate not found for connector 'manzana'",
        "connector": "manzana",
        "trading_pairs": ["BTC-USD"],
    })
    RateNotFoundResponse.model_validate({"detail": single.value.detail})

    with pytest.raises(HTTPException) as batch:
        await get_rates(
            RateRequest(trading_pairs=["BTC-USD", "ETH-USD"], connector="manzana"),
            service,
        )
    CHECK.assertEqual(404, batch.value.status_code)
    CHECK.assertEqual(["BTC-USD", "ETH-USD"], batch.value.detail["trading_pairs"])

    unscoped = await get_single_rate("BTC-USD", None, service)
    CHECK.assertIsNone(unscoped.rate)


@pytest.mark.asyncio
async def test_named_spot_perpetual_reads_reject_but_global_empty_is_preserved():
    from unittest.mock import AsyncMock

    from fastapi import HTTPException

    from models.trading import FundingPaymentFilterRequest, PositionFilterRequest
    from routers.trading import get_funding_payments, get_positions

    class SpotConnectorService:
        def __init__(self):
            self.connector = object()

        def get_all_trading_connectors(self):
            return {"master": {"manzana": self.connector}}

        def connector_supports_perpetual(self, connector_name, connector=None):
            return False

    connector_service = SpotConnectorService()
    accounts_service = types.SimpleNamespace(get_account_positions=AsyncMock())
    history_service = types.SimpleNamespace(get_funding_payments=AsyncMock())

    with pytest.raises(HTTPException) as positions_error:
        await get_positions(
            PositionFilterRequest(
                account_names=["master"], connector_names=["manzana"]
            ),
            accounts_service,
            connector_service,
        )
    CHECK.assertEqual(400, positions_error.value.status_code)
    CHECK.assertEqual("unsupported_connector_feature", positions_error.value.detail["code"])
    CHECK.assertEqual("perpetual", positions_error.value.detail["feature"])

    with pytest.raises(HTTPException) as funding_error:
        await get_funding_payments(
            FundingPaymentFilterRequest(
                account_names=["master"], connector_names=["manzana"]
            ),
            history_service,
            connector_service,
        )
    CHECK.assertEqual(400, funding_error.value.status_code)
    CHECK.assertEqual(0, history_service.get_funding_payments.await_count)
    CHECK.assertEqual(0, accounts_service.get_account_positions.await_count)

    global_positions = await get_positions(
        PositionFilterRequest(), accounts_service, connector_service
    )
    global_funding = await get_funding_payments(
        FundingPaymentFilterRequest(), history_service, connector_service
    )
    CHECK.assertEqual([], global_positions.data)
    CHECK.assertEqual([], global_funding.data)


@pytest.mark.asyncio
async def test_failed_add_market_preserves_markets_membership_exactly():
    from unittest.mock import AsyncMock

    from services.trading_service import AccountTradingInterface

    connector = object()

    class ConnectorService:
        async def get_trading_connector(self, account_name, connector_name):
            return connector

        def get_account_connectors(self, account_name):
            return {"static": connector}

    market_data_service = types.SimpleNamespace(
        initialize_order_book=AsyncMock(return_value=False)
    )
    interface = AccountTradingInterface(
        ConnectorService(), market_data_service, "master"
    )
    interface._markets = {
        "static": {"ETH-USD"},
        "other": {"SOL-USD"},
    }
    expected_markets = {
        "static": {"ETH-USD"},
        "other": {"SOL-USD"},
    }

    with pytest.raises(ValueError, match="Failed to initialize order book"):
        await interface.add_market("static", "BTC-USD")

    CHECK.assertEqual(expected_markets, interface.markets)


@pytest.mark.asyncio
async def test_static_connector_add_rejects_before_initialization():
    from models.market_data import AddTradingPairRequest
    from routers.market_data import add_trading_pair
    from services.connector_capabilities import (
        ConnectorCapabilityError,
        ConnectorCapabilityRegistry,
    )

    class GuardedConnectorService:
        def __init__(self):
            self.registry = ConnectorCapabilityRegistry()
            self.initialize_calls = 0

        def require_capability(self, connector_name, feature, trading_pair=None):
            self.registry.require(connector_name, feature, trading_pair)

        async def initialize_order_book(self, **kwargs):
            self.initialize_calls += 1
            raise AssertionError("static request reached dynamic initialization")

    connector_service = GuardedConnectorService()
    service = MarketDataService(connector_service)

    with pytest.raises(ConnectorCapabilityError) as unsupported:
        await add_trading_pair(
            AddTradingPairRequest(
                connector_name="manzana",
                trading_pair="BTC-USD",
            ),
            service,
        )

    CHECK.assertEqual(400, unsupported.value.status_code)
    CHECK.assertEqual("dynamic_subscription", unsupported.value.detail["feature"])
    CHECK.assertEqual(0, connector_service.initialize_calls)


@pytest.mark.asyncio
async def test_candle_route_checks_capability_before_factory_lookup():
    from models.market_data import CandlesConfigRequest
    from routers.market_data import get_candles
    from services.connector_capabilities import (
        ConnectorCapabilityError,
        ConnectorCapabilityRegistry,
    )

    class GuardedConnectorService:
        def __init__(self):
            self.registry = ConnectorCapabilityRegistry()

        def require_capability(self, connector_name, feature, trading_pair=None):
            self.registry.require(connector_name, feature, trading_pair)

    service = MarketDataService(GuardedConnectorService())
    request = types.SimpleNamespace(
        app=types.SimpleNamespace(state=types.SimpleNamespace(market_data_service=service))
    )

    with pytest.raises(ConnectorCapabilityError) as unsupported:
        await get_candles(
            request,
            CandlesConfigRequest(
                connector_name="manzana",
                trading_pair="BTC-USD",
                interval="1m",
                max_records=10,
            ),
        )

    CHECK.assertEqual(400, unsupported.value.status_code)
    CHECK.assertEqual("candles", unsupported.value.detail["feature"])


def test_openapi_documents_capability_partial_and_canonical_price_contracts():
    from fastapi import FastAPI

    from routers import connectors, market_data, trading

    app = FastAPI()
    app.include_router(connectors.router)
    app.include_router(market_data.router)
    app.include_router(trading.router)
    schema = app.openapi()

    query_schema = schema["components"]["schemas"]["OrderBookQueryResult"]
    CHECK.assertIn("result_price", query_schema["required"])
    CHECK.assertNotIn("average_price", query_schema["properties"])
    CHECK.assertEqual(
        schema["paths"]["/connectors/{connector_name}/capabilities"]["get"]
        ["responses"]["200"]["content"]["application/json"]["schema"]["$ref"]
        , "#/components/schemas/ConnectorCapabilities"
    )
    trading_rule_responses = schema["paths"][
        "/connectors/{connector_name}/trading-rules"
    ]["get"]["responses"]
    CHECK.assertTrue({"207", "400", "404"}.issubset(trading_rule_responses))


class SymmetricPinnedOrderBook:
    """Faithful finite-depth contract for the pinned Cython primitives."""

    asks = ((100.0, 1.0), (110.0, 1.0))
    bids = ((99.0, 1.0), (90.0, 1.0))

    def __init__(self):
        self.calls = []

    def _levels(self, is_buy):
        return self.asks if is_buy else self.bids

    def get_price_for_volume(self, is_buy, volume):
        self.calls.append(("price_for_volume", is_buy, volume))
        cumulative = 0.0
        result_price = float("nan")
        for price, amount in self._levels(is_buy):
            cumulative += amount
            if cumulative >= volume:
                result_price = price
                break
        return QueryResult(float("nan"), volume, result_price, min(cumulative, volume))

    def get_volume_for_price(self, is_buy, price):
        self.calls.append(("volume_for_price", is_buy, price))
        cumulative = 0.0
        result_price = float("nan")
        for level_price, amount in self._levels(is_buy):
            if (is_buy and level_price > price) or (not is_buy and level_price < price):
                break
            cumulative += amount
            result_price = level_price
        return QueryResult(price, float("nan"), result_price, cumulative)

    def get_price_for_quote_volume(self, is_buy, quote_volume):
        self.calls.append(("price_for_quote_volume", is_buy, quote_volume))
        cumulative = 0.0
        result_price = float("nan")
        for price, amount in self._levels(is_buy):
            cumulative += price * amount
            if cumulative >= quote_volume:
                result_price = price
                break
        return QueryResult(float("nan"), quote_volume, result_price, min(cumulative, quote_volume))

    def get_quote_volume_for_price(self, is_buy, price):
        self.calls.append(("quote_volume_for_price", is_buy, price))
        cumulative = 0.0
        result_price = float("nan")
        for level_price, amount in self._levels(is_buy):
            if (is_buy and level_price > price) or (not is_buy and level_price < price):
                break
            cumulative += level_price * amount
            result_price = level_price
        return QueryResult(price, float("nan"), result_price, cumulative)

    def get_vwap_for_volume(self, is_buy, volume):
        self.calls.append(("vwap_for_volume", is_buy, volume))
        total_cost = 0.0
        total_volume = 0.0
        result_price = float("nan")
        for price, amount in self._levels(is_buy):
            total_cost += price * amount
            total_volume += amount
            if total_volume >= volume:
                total_cost -= price * amount
                total_volume -= amount
                consumed = volume - total_volume
                total_cost += price * consumed
                total_volume += consumed
                result_price = total_cost / total_volume
                break
        return QueryResult(float("nan"), volume, result_price, min(total_volume, volume))


@pytest.mark.asyncio
async def test_b1_all_query_paths_require_finite_prices_and_full_depth():
    from services.connector_capabilities import ConnectorCapabilityError

    book = SymmetricPinnedOrderBook()
    service = MarketDataService(OrderBookConnectorService(book))
    exact_cases = (
        ({"volume": 2.0}, 110.0, {"volume": 2.0}, 90.0),
        ({"price": 110.0}, 110.0, {"price": 90.0}, 90.0),
        ({"quote_volume": 210.0}, 110.0, {"quote_volume": 189.0}, 90.0),
        ({"quote_price": 110.0}, 110.0, {"quote_price": 90.0}, 90.0),
        ({"vwap_volume": 2.0}, 105.0, {"vwap_volume": 2.0}, 94.5),
    )
    for buy_kwargs, buy_price, sell_kwargs, sell_price in exact_cases:
        buy = await service.get_order_book_query_result("static", "BTC-USD", True, **buy_kwargs)
        sell = await service.get_order_book_query_result("static", "BTC-USD", False, **sell_kwargs)
        CHECK.assertEqual(buy_price, buy["result_price"])
        CHECK.assertEqual(sell_price, sell["result_price"])
        CHECK.assertTrue(math.isfinite(buy["result_price"]))
        CHECK.assertTrue(math.isfinite(sell["result_price"]))

    insufficient_cases = (
        ({"volume": 3.0}, {"volume": 3.0}),
        ({"price": 99.0}, {"price": 100.0}),
        ({"quote_volume": 211.0}, {"quote_volume": 190.0}),
        ({"quote_price": 99.0}, {"quote_price": 100.0}),
        ({"vwap_volume": 3.0}, {"vwap_volume": 3.0}),
    )
    for buy_kwargs, sell_kwargs in insufficient_cases:
        for is_buy, kwargs in ((True, buy_kwargs), (False, sell_kwargs)):
            with pytest.raises(ConnectorCapabilityError) as error:
                await service.get_order_book_query_result("static", "BTC-USD", is_buy, **kwargs)
            CHECK.assertEqual(400, error.value.status_code)
            CHECK.assertIn(error.value.detail["code"], {"insufficient_order_book_depth", "invalid_order_book_result"})
            CHECK.assertEqual("static", error.value.detail["connector"])
            CHECK.assertEqual("BTC-USD", error.value.detail["trading_pair"])


@pytest.mark.asyncio
async def test_b1_invalid_query_inputs_reject_before_primitive_calls():
    from services.connector_capabilities import ConnectorCapabilityError

    book = SymmetricPinnedOrderBook()
    service = MarketDataService(OrderBookConnectorService(book))
    for key in ("volume", "price", "quote_volume", "quote_price", "vwap_volume"):
        for invalid in (
            0.0,
            -1.0,
            True,
            float("nan"),
            float("inf"),
            float("-inf"),
            Decimal("sNaN"),
        ):
            with pytest.raises(ConnectorCapabilityError) as error:
                await service.get_order_book_query_result(
                    "static", "BTC-USD", True, **{key: invalid}
                )
            CHECK.assertEqual("invalid_order_book_query", error.value.detail["code"])
    CHECK.assertEqual([], book.calls)


@pytest.mark.asyncio
async def test_b2_order_route_and_add_market_reject_before_downstream_work():
    from models.trading import TradeRequest
    from routers.trading import place_trade
    from services.connector_capabilities import ConnectorCapabilityError, ConnectorCapabilityRegistry
    from services.trading_service import AccountTradingInterface

    class GuardedAccountsService:
        def __init__(self):
            self.registry = ConnectorCapabilityRegistry()
            self.place_calls = 0

        def require_capability(self, connector_name, feature, trading_pair=None):
            self.registry.require(connector_name, feature, trading_pair)

        async def place_trade(self, **kwargs):
            self.place_calls += 1
            raise AssertionError("unsupported order reached account/authentication work")

    accounts = GuardedAccountsService()
    request = TradeRequest(
        account_name="master",
        connector_name="manzana",
        trading_pair="BTC-USD",
        trade_type="BUY",
        amount=Decimal("0.1"),
        order_type="LIMIT",
        price=Decimal("100"),
    )
    with pytest.raises(ConnectorCapabilityError) as order_error:
        await place_trade(request, accounts)
    CHECK.assertEqual("order_submission", order_error.value.detail["feature"])
    CHECK.assertEqual(0, accounts.place_calls)

    class GuardedConnectorService:
        def __init__(self):
            self.ensure_calls = 0

        async def get_trading_connector(self, account_name, connector_name):
            self.ensure_calls += 1
            raise AssertionError("unsupported market reached connector creation")

        def get_account_connectors(self, account_name):
            return {}

    connector_service = GuardedConnectorService()
    market_data_service = MarketDataService(
        types.SimpleNamespace(
            require_capability=ConnectorCapabilityRegistry().require,
        )
    )
    interface = AccountTradingInterface(connector_service, market_data_service, "master")
    with pytest.raises(ConnectorCapabilityError):
        await interface.add_market("manzana", "BTC-USD")
    CHECK.assertEqual(0, connector_service.ensure_calls)


def test_b2_unified_service_resolves_cached_instance_declaration():
    from services.connector_capabilities import (
        ConnectorCapabilityError,
        ConnectorCapabilityRegistry,
        ConnectorFeature,
    )

    config_crypt = _module("hummingbot.client.config.config_crypt")
    config_crypt.ETHKeyFileSecretManger = type("ETHKeyFileSecretManger", (), {})
    config_helpers = _module("hummingbot.client.config.config_helpers")
    config_helpers.ClientConfigAdapter = type("ClientConfigAdapter", (), {})
    config_helpers.api_keys_from_connector_config_map = lambda value: {}
    config_helpers.get_connector_class = lambda name: object
    metrics = _module("hummingbot.connector.connector_metrics_collector")
    metrics.TradeVolumeMetricCollector = type("TradeVolumeMetricCollector", (), {})
    exchange = _module("hummingbot.connector.exchange_py_base")
    exchange.ExchangePyBase = type("ExchangePyBase", (), {})
    gateway_package = _module("hummingbot.connector.gateway")
    gateway_package.__path__ = []
    gateway = _module("hummingbot.connector.gateway.gateway")
    gateway.Gateway = type("Gateway", (), {})
    perpetual = _module("hummingbot.connector.perpetual_derivative_py_base")
    perpetual.PerpetualDerivativePyBase = type("PerpetualDerivativePyBase", (), {})
    in_flight = _module("hummingbot.core.data_type.in_flight_order")
    in_flight.InFlightOrder = type("InFlightOrder", (), {})
    in_flight.OrderState = Enum(
        "OrderState",
        "PENDING_CREATE OPEN PENDING_CANCEL CANCELED PARTIALLY_FILLED FILLED FAILED "
        "PENDING_APPROVAL APPROVED CREATED COMPLETED",
    )
    core_utils = _module("hummingbot.core.utils")
    core_utils.__path__ = []
    async_utils = _module("hummingbot.core.utils.async_utils")
    async_utils.safe_ensure_future = asyncio.ensure_future
    file_system = _module("utils.file_system")
    file_system.fs_util = types.SimpleNamespace()
    config_adapter = _module("utils.hummingbot_api_config_adapter")
    config_adapter.HummingbotAPIConfigAdapter = type("HummingbotAPIConfigAdapter", (), {})
    security = _module("utils.security")
    security.BackendAPISecurity = type("BackendAPISecurity", (), {})

    module_name = "issue222_unified_connector_service_under_test"
    spec = importlib.util.spec_from_file_location(
        module_name,
        Path(__file__).resolve().parents[1] / "services" / "unified_connector_service.py",
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    service = object.__new__(module.UnifiedConnectorService)
    service._capability_registry = ConnectorCapabilityRegistry()
    service._data_connectors = {
        "custom": types.SimpleNamespace(
            api_capabilities={
                "supported_pairs": {"ONE-USD"},
                "features": {ConnectorFeature.ORDER_BOOK.value},
                "candles": False,
            }
        )
    }
    service._trading_connectors = {}
    with pytest.raises(ConnectorCapabilityError):
        service.require_capability("custom", ConnectorFeature.CANDLES, "ONE-USD")


def _router_contract_app():
    from fastapi import FastAPI
    from routers import connectors, market_data, trading

    app = FastAPI()
    app.include_router(connectors.router)
    app.include_router(market_data.router)
    app.include_router(trading.router)
    return app, connectors, market_data, trading


def test_b3_asgi_typed_capability_errors_match_openapi_models():
    from fastapi.testclient import TestClient
    from models.connectors import CapabilityErrorResponse
    from services.connector_capabilities import ConnectorCapabilityRegistry

    app, _, market_data_router, trading_router = _router_contract_app()

    class ConnectorService:
        registry = ConnectorCapabilityRegistry()

        def get_connector_capabilities(self, connector_name):
            return self.registry.get(connector_name)

        def is_known_connector(self, connector_name):
            return False

    app.state.market_data_service = types.SimpleNamespace(
        connector_service=ConnectorService()
    )

    class FundingService:
        def __init__(self):
            self.funding_calls = 0

        def require_capability(self, connector_name, feature, trading_pair=None):
            return None

        async def get_funding_info(self, connector_name, trading_pair):
            self.funding_calls += 1
            return {"error": "must not be called"}

    funding = FundingService()
    app.dependency_overrides[market_data_router.get_market_data_service] = lambda: funding

    class OrderAccounts:
        def __init__(self):
            self.registry = ConnectorCapabilityRegistry()
            self.place_calls = 0

        def require_capability(self, connector_name, feature, trading_pair=None):
            self.registry.require(connector_name, feature, trading_pair)

        async def place_trade(self, **kwargs):
            self.place_calls += 1
            raise AssertionError("unsupported order reached submission")

    order_accounts = OrderAccounts()
    app.dependency_overrides[trading_router.get_accounts_service] = lambda: order_accounts
    client = TestClient(app)

    unknown = client.get("/connectors/unknown/capabilities")
    CHECK.assertEqual(404, unknown.status_code)
    CapabilityErrorResponse.model_validate(unknown.json())
    CHECK.assertEqual("unknown_connector", unknown.json()["detail"]["code"])

    suffix = client.post(
        "/market-data/funding-info",
        json={"connector_name": "generic_spot", "trading_pair": "BTC-USD"},
    )
    CHECK.assertEqual(400, suffix.status_code)
    CapabilityErrorResponse.model_validate(suffix.json())
    CHECK.assertEqual("unsupported_connector_feature", suffix.json()["detail"]["code"])
    CHECK.assertEqual(0, funding.funding_calls)

    order = client.post(
        "/trading/orders",
        json={
            "account_name": "master",
            "connector_name": "manzana",
            "trading_pair": "BTC-USD",
            "trade_type": "BUY",
            "amount": "0.1",
            "order_type": "LIMIT",
            "price": "100",
        },
    )
    CHECK.assertEqual(400, order.status_code)
    CapabilityErrorResponse.model_validate(order.json())
    CHECK.assertEqual("order_submission", order.json()["detail"]["feature"])
    CHECK.assertEqual(0, order_accounts.place_calls)

    order_responses = app.openapi()["paths"]["/trading/orders"]["post"]["responses"]
    CHECK.assertEqual(
        "#/components/schemas/CapabilityErrorResponse",
        order_responses["400"]["content"]["application/json"]["schema"]["$ref"],
    )


@pytest.mark.asyncio
async def test_b4_named_unloaded_spot_reads_reject_without_querying():
    from unittest.mock import AsyncMock

    from models.trading import FundingPaymentFilterRequest, PositionFilterRequest
    from routers.trading import get_funding_payments, get_positions
    from services.connector_capabilities import ConnectorCapabilityRegistry

    class UnloadedConnectorService:
        def __init__(self):
            self.registry = ConnectorCapabilityRegistry()

        def get_all_trading_connectors(self):
            return {"master": {}}

        def get_connector_capabilities(self, connector_name):
            return self.registry.get(connector_name)

        def connector_supports_perpetual(self, connector_name, connector=None):
            capabilities = self.registry.get(connector_name, connector)
            if capabilities.declared:
                return bool(capabilities.perpetual)
            return connector_name.endswith("_perpetual")

    connector_service = UnloadedConnectorService()
    accounts_service = types.SimpleNamespace(get_account_positions=AsyncMock())
    history_service = types.SimpleNamespace(get_funding_payments=AsyncMock())
    with pytest.raises(Exception) as positions_error:
        await get_positions(
            PositionFilterRequest(account_names=["master"], connector_names=["manzana"]),
            accounts_service,
            connector_service,
        )
    CHECK.assertEqual(400, positions_error.value.status_code)
    with pytest.raises(Exception) as funding_error:
        await get_funding_payments(
            FundingPaymentFilterRequest(account_names=["master"], connector_names=["manzana"]),
            history_service,
            connector_service,
        )
    CHECK.assertEqual(400, funding_error.value.status_code)
    CHECK.assertEqual(0, accounts_service.get_account_positions.await_count)
    CHECK.assertEqual(0, history_service.get_funding_payments.await_count)

    connector_service.get_all_trading_connectors = lambda: {}
    with pytest.raises(Exception) as globally_unloaded:
        await get_positions(
            PositionFilterRequest(connector_names=["manzana"]),
            accounts_service,
            connector_service,
        )
    CHECK.assertEqual(400, globally_unloaded.value.status_code)

    global_positions = await get_positions(
        PositionFilterRequest(), accounts_service, connector_service
    )
    CHECK.assertEqual([], global_positions.data)


class TransactionTracker:
    def __init__(self, pair=None, book=None):
        self._trading_pairs = [] if pair is None else [pair]
        self.order_books = {} if pair is None else ({pair: book} if book is not None else {})
        self._tracking_message_queues = {}
        self._saved_message_queues = {}
        self._tracking_tasks = {}


class TransactionConnector:
    def __init__(self, tracker, pair=None):
        self.order_book_tracker = tracker
        self._trading_pairs = [] if pair is None else [pair]


class TransactionConnectorService:
    def __init__(self, connector):
        self.connector = connector
        self.stop_calls = 0

    async def get_trading_connector(self, account_name, connector_name):
        return self.connector

    def get_account_connectors(self, account_name):
        return {"generic": self.connector}

    async def stop_trading_connector(self, account_name, connector_name):
        self.stop_calls += 1


def _transaction_state(connector):
    tracker = connector.order_book_tracker
    return {
        "connector_pairs": list(connector._trading_pairs),
        "tracker_pairs": list(tracker._trading_pairs),
        "books": dict(tracker.order_books),
        "queues": dict(tracker._tracking_message_queues),
        "saved": dict(tracker._saved_message_queues),
        "tasks": dict(tracker._tracking_tasks),
    }


@pytest.mark.asyncio
async def test_b5_add_market_rolls_back_partial_absent_and_preexisting_state():
    from services.trading_service import AccountTradingInterface

    for preexisting in (False, True):
        old_book = object() if preexisting else None
        tracker = TransactionTracker("BTC-USD" if preexisting else None, old_book)
        connector = TransactionConnector(tracker, "BTC-USD" if preexisting else None)
        connector_service = TransactionConnectorService(connector)
        before = _transaction_state(connector)

        async def initialize_order_book(**kwargs):
            connector._trading_pairs.append("BTC-USD")
            tracker._trading_pairs.append("BTC-USD")
            tracker.order_books["BTC-USD"] = object()
            tracker._tracking_message_queues["BTC-USD"] = asyncio.Queue()
            tracker._saved_message_queues["BTC-USD"] = ["candidate"]
            return False

        remove_calls = []

        async def remove_trading_pair(**kwargs):
            remove_calls.append(kwargs)
            return False

        market_data = types.SimpleNamespace(
            initialize_order_book=initialize_order_book,
            remove_trading_pair=remove_trading_pair,
        )
        interface = AccountTradingInterface(connector_service, market_data, "master")
        if preexisting:
            interface._markets = {"generic": {"BTC-USD"}}
        expected_markets = {key: set(value) for key, value in interface._markets.items()}
        with pytest.raises(ValueError):
            await interface.add_market("generic", "BTC-USD")
        CHECK.assertEqual(expected_markets, interface.markets)
        CHECK.assertEqual(before, _transaction_state(connector))
        CHECK.assertEqual(0 if preexisting else 1, len(remove_calls))

    tracker = TransactionTracker()
    connector = TransactionConnector(tracker)

    class NewlyCreatedConnectorService:
        def __init__(self):
            self.loaded = False
            self.stop_calls = 0

        async def get_trading_connector(self, account_name, connector_name):
            self.loaded = True
            return connector

        def get_account_connectors(self, account_name):
            return {"generic": connector} if self.loaded else {}

        async def stop_trading_connector(self, account_name, connector_name):
            self.stop_calls += 1
            self.loaded = False

    newly_created_service = NewlyCreatedConnectorService()

    async def fail_new_connector(**kwargs):
        connector._trading_pairs.append("BTC-USD")
        tracker._trading_pairs.append("BTC-USD")
        tracker.order_books["BTC-USD"] = object()
        return False

    interface = AccountTradingInterface(
        newly_created_service,
        types.SimpleNamespace(initialize_order_book=fail_new_connector),
        "master",
    )
    with pytest.raises(ValueError):
        await interface.add_market("generic", "BTC-USD")
    CHECK.assertEqual(1, newly_created_service.stop_calls)
    CHECK.assertEqual({}, newly_created_service.get_account_connectors("master"))
    CHECK.assertEqual(
        {
            "connector_pairs": [],
            "tracker_pairs": [],
            "books": {},
            "queues": {},
            "saved": {},
            "tasks": {},
        },
        _transaction_state(connector),
    )


@pytest.mark.asyncio
async def test_b5_add_market_serializes_concurrent_callers_and_cancellation():
    from services.trading_service import AccountTradingInterface

    class ReadyBook:
        snapshot = ([1], [1])

    tracker = TransactionTracker()
    connector = TransactionConnector(tracker)
    connector_service = TransactionConnectorService(connector)
    initialize_calls = 0

    async def initialize_success(**kwargs):
        nonlocal initialize_calls
        initialize_calls += 1
        await asyncio.sleep(0)
        tracker.order_books["BTC-USD"] = ReadyBook()
        tracker._trading_pairs.append("BTC-USD")
        return True

    interface = AccountTradingInterface(
        connector_service,
        types.SimpleNamespace(initialize_order_book=initialize_success),
        "master",
    )
    await asyncio.gather(
        interface.add_market("generic", "BTC-USD"),
        interface.add_market("generic", "BTC-USD"),
    )
    CHECK.assertEqual(1, initialize_calls)

    cancel_tracker = TransactionTracker()
    cancel_connector = TransactionConnector(cancel_tracker)
    cancel_service = TransactionConnectorService(cancel_connector)
    entered = asyncio.Event()
    blocker = asyncio.Event()
    leaked_task = None

    async def initialize_cancel(**kwargs):
        nonlocal leaked_task
        cancel_connector._trading_pairs.append("BTC-USD")
        cancel_tracker._trading_pairs.append("BTC-USD")
        cancel_tracker.order_books["BTC-USD"] = object()
        leaked_task = asyncio.create_task(blocker.wait())
        cancel_tracker._tracking_tasks["BTC-USD"] = leaked_task
        entered.set()
        await blocker.wait()
        return True

    cancel_interface = AccountTradingInterface(
        cancel_service,
        types.SimpleNamespace(initialize_order_book=initialize_cancel),
        "master",
    )
    task = asyncio.create_task(cancel_interface.add_market("generic", "BTC-USD"))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    CHECK.assertEqual([], cancel_connector._trading_pairs)
    CHECK.assertEqual([], cancel_tracker._trading_pairs)
    CHECK.assertEqual({}, cancel_tracker.order_books)
    CHECK.assertEqual({}, cancel_tracker._tracking_tasks)
    CHECK.assertTrue(leaked_task is None or leaked_task.done())
    if leaked_task is not None and not leaked_task.done():
        leaked_task.cancel()
        await asyncio.gather(leaked_task, return_exceptions=True)


def test_b6_capability_json_is_deterministic_across_hash_seeds():
    repo = Path(__file__).resolve().parents[1]
    model_path = repo / "models" / "connectors.py"
    script = """
import importlib.util
spec = importlib.util.spec_from_file_location('issue222_connector_models', r'%s')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
value = module.ConnectorCapabilities(
    declared=True,
    supported_pairs=frozenset({'ETH-USD', 'BTC-USD'}),
    features=frozenset({'rates', 'tickers', 'order_book', 'trading_rules', 'order_book_query'}),
)
print(value.model_dump_json())
""" % str(model_path)
    outputs = []
    for seed in ("1", "2", "3", "4", "5"):
        env = os.environ.copy()
        env.pop("PYTHONPATH", None)
        env["PYTHONHASHSEED"] = seed
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        result = subprocess.run(
            [sys.executable, "-B", "-c", script],
            cwd=repo,
            env=env,
            check=True,
            capture_output=True,
            text=True,
        )
        outputs.append(result.stdout.strip())
    CHECK.assertEqual(1, len(set(outputs)), outputs)


def test_b7_contract_module_does_not_pollute_imports_or_use_bare_asserts():
    polluted = sorted(
        name
        for name, module in sys.modules.items()
        if getattr(module, "__issue222_contract_shim__", False)
    )
    CHECK.assertEqual([], polluted)
    tree = ast.parse(Path(__file__).read_text(encoding="utf-8"))
    bare_asserts = [node.lineno for node in ast.walk(tree) if isinstance(node, ast.Assert)]
    CHECK.assertEqual([], bare_asserts)
