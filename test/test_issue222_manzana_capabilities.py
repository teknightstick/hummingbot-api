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

    class CandlesConfig:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    candles_types.CandlesConfig = CandlesConfig
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
        "test_final_b8_fixture_restores_preexisting_module_dicts",
    }:
        yield
        return

    before = {
        name: (module, dict(module.__dict__))
        for name, module in sys.modules.items()
        if _is_contract_module(name)
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
        for name, (module, attributes) in before.items():
            sys.modules[name] = module
            module.__dict__.clear()
            module.__dict__.update(attributes)


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
    CHECK.assertEqual(
        "dynamic_subscription", readiness_error.value.detail["feature"]
    )
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
        initialize_order_book=AsyncMock(return_value=False),
        remove_trading_pair=AsyncMock(return_value=True),
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
    with pytest.raises(ConnectorCapabilityError):
        await interface.ensure_connector("manzana")
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
            self.connector_map_reads = 0

        def get_all_trading_connectors(self):
            self.connector_map_reads += 1
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
    CHECK.assertEqual(0, connector_service.connector_map_reads)

    with pytest.raises(Exception) as globally_unloaded:
        await get_positions(
            PositionFilterRequest(connector_names=["manzana"]),
            accounts_service,
            connector_service,
        )
    CHECK.assertEqual(400, globally_unloaded.value.status_code)
    CHECK.assertEqual(0, connector_service.connector_map_reads)

    connector_service.get_all_trading_connectors = lambda: {}
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
    from services.connector_capabilities import ConnectorCapabilityError
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
        with pytest.raises(ConnectorCapabilityError):
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


def test_final_b1_boolean_json_is_rejected_before_all_query_primitives():
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from pydantic import ValidationError

    from models.market_data import (
        PriceForQuoteVolumeRequest,
        PriceForVolumeRequest,
        QuoteVolumeForPriceRequest,
        VolumeForPriceRequest,
        VWAPForVolumeRequest,
    )
    from routers import market_data as market_data_router

    class QueryService:
        def __init__(self):
            self.calls = []

        async def get_order_book_query_result(self, connector, pair, is_buy, **kwargs):
            self.calls.append((connector, pair, is_buy, kwargs))
            value = next(iter(kwargs.values()))
            return {
                "trading_pair": pair,
                "is_buy": is_buy,
                "query_volume": float(value),
                "result_price": 100.0,
                "result_volume": float(value),
                "timestamp": 1.0,
            }

    service = QueryService()
    app = FastAPI()
    app.include_router(market_data_router.router)
    app.dependency_overrides[market_data_router.get_market_data_service] = lambda: service
    client = TestClient(app)
    cases = (
        ("/market-data/order-book/price-for-volume", "volume", PriceForVolumeRequest),
        ("/market-data/order-book/volume-for-price", "price", VolumeForPriceRequest),
        (
            "/market-data/order-book/price-for-quote-volume",
            "quote_volume",
            PriceForQuoteVolumeRequest,
        ),
        (
            "/market-data/order-book/quote-volume-for-price",
            "price",
            QuoteVolumeForPriceRequest,
        ),
        ("/market-data/order-book/vwap-for-volume", "volume", VWAPForVolumeRequest),
    )
    base = {"connector_name": "generic", "trading_pair": "BTC-USD", "is_buy": True}
    for path, field, model in cases:
        calls_before_booleans = len(service.calls)
        for boolean in (True, False):
            response = client.post(path, json={**base, field: boolean})
            CHECK.assertEqual(422, response.status_code, (path, boolean, response.text))
        CHECK.assertEqual(calls_before_booleans, len(service.calls))

        for number in (1, 1.5):
            response = client.post(path, json={**base, field: number})
            CHECK.assertEqual(200, response.status_code, (path, number, response.text))
        model(**base, **{field: Decimal("1.25")})
        for invalid in (
            0,
            -1,
            float("nan"),
            float("inf"),
            float("-inf"),
            Decimal("0"),
            Decimal("-1"),
            Decimal("NaN"),
            Decimal("Infinity"),
            Decimal("-Infinity"),
        ):
            with pytest.raises(ValidationError):
                model(**base, **{field: invalid})

    CHECK.assertEqual(10, len(service.calls))


def test_final_b2_all_adjacent_live_and_order_routes_gate_before_work():
    from fastapi.testclient import TestClient

    from models.connectors import CapabilityErrorResponse
    from services.connector_capabilities import ConnectorCapabilityRegistry

    app, _, market_data_router, trading_router = _router_contract_app()

    class Downstream:
        def __init__(self):
            self.registry = ConnectorCapabilityRegistry()
            self.calls = []
            self.connector_service = self

        def require_capability(self, connector_name, feature, trading_pair=None):
            self.registry.require(connector_name, feature, trading_pair)

        def get_data_connector(self, connector_name):
            self.calls.append("order_types")
            return types.SimpleNamespace(
                supported_order_types=lambda: [types.SimpleNamespace(name="MARKET")]
            )

        async def cancel_order(self, **kwargs):
            self.calls.append("cancel")
            return kwargs["client_order_id"]

        def get_order_book_tracker_diagnostics(self, **kwargs):
            self.calls.append("diagnostics")
            return {"tracker_ready": True}

        async def restart_order_book_tracker(self, **kwargs):
            self.calls.append("restart")
            return {"success": True}

        async def remove_trading_pair(self, **kwargs):
            self.calls.append("remove_pair")
            return True

        def get_all_trading_connectors(self):
            self.calls.append("active_orders")
            return {}

    downstream = Downstream()
    app.state.market_data_service = downstream
    app.dependency_overrides[market_data_router.get_market_data_service] = lambda: downstream
    app.dependency_overrides[trading_router.get_accounts_service] = lambda: downstream
    app.dependency_overrides[trading_router.get_connector_service] = lambda: downstream
    client = TestClient(app)
    requests = (
        ("get", "/connectors/manzana/order-types", None),
        ("post", "/trading/master/manzana/orders/order-1/cancel", None),
        ("get", "/market-data/order-book/diagnostics/manzana", None),
        ("post", "/market-data/order-book/restart/manzana", None),
        (
            "post",
            "/market-data/trading-pair/remove",
            {"connector_name": "manzana", "trading_pair": "BTC-USD"},
        ),
        (
            "post",
            "/trading/orders/active",
            {"account_names": ["master"], "connector_names": ["manzana"]},
        ),
    )
    for method, path, body in requests:
        response = getattr(client, method)(path, json=body) if body is not None else getattr(client, method)(path)
        CHECK.assertEqual(400, response.status_code, (path, response.text))
        CapabilityErrorResponse.model_validate(response.json())
    CHECK.assertEqual([], downstream.calls)

    schema = app.openapi()["paths"]
    schema_paths = {
        "/connectors/manzana/order-types": "/connectors/{connector_name}/order-types",
        "/trading/master/manzana/orders/order-1/cancel": (
            "/trading/{account_name}/{connector_name}/orders/{client_order_id}/cancel"
        ),
        "/market-data/order-book/diagnostics/manzana": (
            "/market-data/order-book/diagnostics/{connector_name}"
        ),
        "/market-data/order-book/restart/manzana": (
            "/market-data/order-book/restart/{connector_name}"
        ),
    }
    for method, path, _ in requests:
        responses = schema[schema_paths.get(path, path)][method]["responses"]
        CHECK.assertEqual(
            "#/components/schemas/CapabilityErrorResponse",
            responses["400"]["content"]["application/json"]["schema"]["$ref"],
        )


def test_final_b3_all_advertised_unsupported_bodies_match_openapi():
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from models.connectors import (
        CapabilityErrorResponse,
        TradingRulesNotFoundResponse,
        TradingRulesPartialResponse,
    )
    from routers import connectors as connectors_router
    from routers import market_data as market_data_router

    class UnsupportedService:
        def require_capability(self, connector_name, feature, trading_pair=None):
            return None

        async def get_candles_feed(self, config):
            raise market_data_router.UnsupportedConnectorException("unsupported candle source")

        async def get_funding_info(self, connector_name, trading_pair):
            return {"error": "Funding info not supported by this connector"}

    service = UnsupportedService()
    app = FastAPI()
    app.include_router(market_data_router.router)
    app.state.market_data_service = service
    app.dependency_overrides[market_data_router.get_market_data_service] = lambda: service
    client = TestClient(app)
    original_map = market_data_router.CandlesFactory._candles_map
    try:
        market_data_router.CandlesFactory._candles_map = {}
        unknown_requests = (
            ("/market-data/candles", {"connector_name": "unknown", "trading_pair": "BTC-USD"}),
            (
                "/market-data/historical-candles",
                {"connector_name": "unknown", "trading_pair": "BTC-USD", "interval": "1m"},
            ),
        )
        responses = [client.post(path, json=body) for path, body in unknown_requests]

        market_data_router.CandlesFactory._candles_map = {"generic": object()}
        responses.extend(
            [
                client.post(
                    "/market-data/candles",
                    json={"connector_name": "generic", "trading_pair": "BTCUSD"},
                ),
                client.post(
                    "/market-data/historical-candles",
                    json={"connector_name": "generic", "trading_pair": "BTCUSD", "interval": "1m"},
                ),
                client.post(
                    "/market-data/candles",
                    json={"connector_name": "generic", "trading_pair": "BTC-USD"},
                ),
                client.post(
                    "/market-data/historical-candles",
                    json={"connector_name": "generic", "trading_pair": "BTC-USD", "interval": "1m"},
                ),
            ]
        )
        responses.append(
            client.post(
                "/market-data/funding-info",
                json={"connector_name": "generic_perpetual", "trading_pair": "BTC-USD"},
            )
        )
    finally:
        market_data_router.CandlesFactory._candles_map = original_map

    for response in responses:
        CHECK.assertEqual(400, response.status_code, response.text)
        CapabilityErrorResponse.model_validate(response.json())

    schema = app.openapi()["paths"]
    for path in (
        "/market-data/candles",
        "/market-data/historical-candles",
        "/market-data/funding-info",
    ):
        CHECK.assertEqual(
            "#/components/schemas/CapabilityErrorResponse",
            schema[path]["post"]["responses"]["400"]["content"]["application/json"]["schema"]["$ref"],
        )

    class RulesService:
        async def get_trading_rules(self, connector_name, trading_pairs):
            return {
                pair: (
                    {"min_order_size": 0.001}
                    if pair == "BTC-USD"
                    else {"error": "missing"}
                )
                for pair in trading_pairs
            }

    rules_app = FastAPI()
    rules_app.include_router(connectors_router.router)
    rules_app.state.market_data_service = RulesService()
    rules_client = TestClient(rules_app)
    partial = rules_client.get(
        "/connectors/generic/trading-rules",
        params=(("trading_pairs", "BTC-USD"), ("trading_pairs", "ETH-USD")),
    )
    missing = rules_client.get(
        "/connectors/generic/trading-rules",
        params=(("trading_pairs", "ETH-USD"),),
    )
    CHECK.assertEqual(207, partial.status_code, partial.text)
    TradingRulesPartialResponse.model_validate(partial.json())
    CHECK.assertEqual(404, missing.status_code, missing.text)
    TradingRulesNotFoundResponse.model_validate(missing.json())
    rules_responses = rules_app.openapi()["paths"][
        "/connectors/{connector_name}/trading-rules"
    ]["get"]["responses"]
    CHECK.assertEqual(
        "#/components/schemas/TradingRulesPartialResponse",
        rules_responses["207"]["content"]["application/json"]["schema"]["$ref"],
    )
    CHECK.assertEqual(
        "#/components/schemas/TradingRulesNotFoundResponse",
        rules_responses["404"]["content"]["application/json"]["schema"]["$ref"],
    )


@pytest.mark.asyncio
async def test_final_b4_cross_pair_failures_cannot_erase_successes():
    from services.trading_service import AccountTradingInterface

    class ReadyBook:
        snapshot = ([1], [1])

    tracker = TransactionTracker()
    connector = TransactionConnector(tracker)
    connector_service = TransactionConnectorService(connector)
    a_started = asyncio.Event()

    async def initialize_order_book(**kwargs):
        pair = kwargs["trading_pair"]
        connector._trading_pairs.append(pair)
        tracker._trading_pairs.append(pair)
        tracker.order_books[pair] = ReadyBook()
        if pair == "A-USD":
            a_started.set()
            await asyncio.sleep(0.05)
            return False
        return True

    interface = AccountTradingInterface(
        connector_service,
        types.SimpleNamespace(
            initialize_order_book=initialize_order_book,
            remove_trading_pair=lambda **kwargs: asyncio.sleep(0, result=True),
        ),
        "master",
    )
    failing = asyncio.create_task(interface.add_market("generic", "A-USD"))
    await a_started.wait()
    succeeding = asyncio.create_task(interface.add_market("generic", "B-USD"))
    results = await asyncio.gather(failing, succeeding, return_exceptions=True)
    CHECK.assertIsInstance(results[0], ValueError)
    CHECK.assertIsNone(results[1])
    CHECK.assertEqual({"generic": {"B-USD"}}, interface.markets)
    CHECK.assertEqual(["B-USD"], connector._trading_pairs)
    CHECK.assertEqual(["B-USD"], tracker._trading_pairs)
    CHECK.assertEqual(["B-USD"], list(tracker.order_books))
    CHECK.assertEqual({}, interface._market_locks)

    # Both callers begin while the connector is absent. Connector-wide serialization
    # makes A's owned rollback complete before B creates and commits its state.
    absent_tracker = TransactionTracker()
    absent_connector = TransactionConnector(absent_tracker)

    class AbsentConnectorService:
        def __init__(self):
            self.loaded = False
            self.stop_calls = 0

        async def get_trading_connector(self, account_name, connector_name):
            self.loaded = True
            return absent_connector

        def get_account_connectors(self, account_name):
            return {"generic": absent_connector} if self.loaded else {}

        async def stop_trading_connector(self, account_name, connector_name):
            self.stop_calls += 1
            self.loaded = False
            absent_connector._trading_pairs.clear()
            absent_tracker._trading_pairs.clear()
            absent_tracker.order_books.clear()
            return True

    absent_service = AbsentConnectorService()
    absent_a_started = asyncio.Event()

    async def initialize_absent_order_book(**kwargs):
        pair = kwargs["trading_pair"]
        absent_connector._trading_pairs.append(pair)
        absent_tracker._trading_pairs.append(pair)
        absent_tracker.order_books[pair] = ReadyBook()
        if pair == "A-USD":
            absent_a_started.set()
            await asyncio.sleep(0.05)
            return False
        return True

    absent_interface = AccountTradingInterface(
        absent_service,
        types.SimpleNamespace(initialize_order_book=initialize_absent_order_book),
        "master",
    )
    absent_failing = asyncio.create_task(absent_interface.add_market("generic", "A-USD"))
    await absent_a_started.wait()
    absent_succeeding = asyncio.create_task(absent_interface.add_market("generic", "B-USD"))
    absent_results = await asyncio.gather(
        absent_failing, absent_succeeding, return_exceptions=True
    )
    CHECK.assertIsInstance(absent_results[0], ValueError)
    CHECK.assertIsNone(absent_results[1])
    CHECK.assertEqual(1, absent_service.stop_calls)
    CHECK.assertTrue(absent_service.loaded)
    CHECK.assertEqual({"generic": {"B-USD"}}, absent_interface.markets)
    CHECK.assertEqual(["B-USD"], absent_connector._trading_pairs)
    CHECK.assertEqual(["B-USD"], absent_tracker._trading_pairs)
    CHECK.assertEqual(["B-USD"], list(absent_tracker.order_books))
    CHECK.assertEqual({}, absent_interface._market_locks)


@pytest.mark.asyncio
async def test_final_b5_cleanup_residuals_are_typed_observable_and_retryable():
    from services.connector_capabilities import ConnectorCapabilityError
    from services.trading_service import AccountTradingInterface

    class ReadyBook:
        snapshot = ([1], [1])

    tracker = TransactionTracker()
    connector = TransactionConnector(tracker)
    connector_service = TransactionConnectorService(connector)
    cleanup_mode = "false"
    initialize_success = False
    cleanup_attempts = []

    async def initialize_order_book(**kwargs):
        pair = kwargs["trading_pair"]
        connector._trading_pairs.append(pair)
        tracker._trading_pairs.append(pair)
        tracker.order_books[pair] = ReadyBook()
        return initialize_success

    async def remove_trading_pair(**kwargs):
        cleanup_attempts.append(cleanup_mode)
        if cleanup_mode == "false":
            return False
        if cleanup_mode == "raise":
            raise RuntimeError("cleanup failed")
        if cleanup_mode == "cancel":
            raise asyncio.CancelledError()
        return True

    market_data = types.SimpleNamespace(
        initialize_order_book=initialize_order_book,
        remove_trading_pair=remove_trading_pair,
    )
    interface = AccountTradingInterface(connector_service, market_data, "master")
    for mode in ("false", "raise", "cancel"):
        cleanup_mode = mode
        with pytest.raises(ConnectorCapabilityError) as error:
            await interface.add_market("generic", "BTC-USD")
        CHECK.assertEqual("market_cleanup_residual", error.value.detail["code"])
        CHECK.assertIn(("generic", "BTC-USD"), interface.cleanup_residuals)
        CHECK.assertEqual([], connector._trading_pairs)
        CHECK.assertEqual([], tracker._trading_pairs)
        CHECK.assertEqual({}, tracker.order_books)

    cleanup_mode = "success"
    initialize_success = True
    await interface.add_market("generic", "BTC-USD")
    CHECK.assertEqual({}, interface.cleanup_residuals)
    CHECK.assertEqual(["false", "raise", "cancel", "success"], cleanup_attempts)

    cancel_tracker = TransactionTracker()
    cancel_connector = TransactionConnector(cancel_tracker)
    cancel_service = TransactionConnectorService(cancel_connector)
    entered = asyncio.Event()

    async def cancellable_initialize(**kwargs):
        cancel_connector._trading_pairs.append("ETH-USD")
        cancel_tracker._trading_pairs.append("ETH-USD")
        cancel_tracker.order_books["ETH-USD"] = ReadyBook()
        entered.set()
        await asyncio.Event().wait()

    cancel_interface = AccountTradingInterface(
        cancel_service,
        types.SimpleNamespace(
            initialize_order_book=cancellable_initialize,
            remove_trading_pair=lambda **kwargs: asyncio.sleep(0, result=False),
        ),
        "master",
    )
    task = asyncio.create_task(cancel_interface.add_market("generic", "ETH-USD"))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    CHECK.assertIn(("generic", "ETH-USD"), cancel_interface.cleanup_residuals)
    CHECK.assertEqual([], cancel_connector._trading_pairs)
    CHECK.assertEqual([], cancel_tracker._trading_pairs)
    CHECK.assertEqual({}, cancel_tracker.order_books)


def _load_unified_service_module():
    from services.connector_capabilities import ConnectorCapabilityRegistry

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
    in_flight.OrderState = Enum("OrderState", "OPEN")
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

    module_name = "issue222_final_unified_connector_service_under_test"
    spec = importlib.util.spec_from_file_location(
        module_name,
        Path(__file__).resolve().parents[1] / "services" / "unified_connector_service.py",
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, ConnectorCapabilityRegistry


@pytest.mark.asyncio
async def test_final_b6_failed_connector_creation_restores_indices_and_drains_tasks():
    module, ConnectorCapabilityRegistry = _load_unified_service_module()

    class StopIndex:
        def __init__(self):
            self.stopped = False

        async def stop(self):
            self.stopped = True

    class MetricIndex:
        def __init__(self):
            self.stopped = False

        def stop(self):
            self.stopped = True

    blocker = asyncio.Event()
    spawned = []

    class Connector:
        order_book_tracker = None
        in_flight_orders = {}

        async def _initialize_trading_pair_symbol_map(self):
            return None

        async def _update_trading_rules(self):
            return None

        async def _update_balances(self):
            return None

        async def stop_network(self):
            return None

    service = object.__new__(module.UnifiedConnectorService)
    service._trading_connectors = {}
    service._connector_locks = {}
    service._connector_lock_users = {}
    service._connector_locks_guard = asyncio.Lock()
    service._orders_recorders = {}
    service._funding_recorders = {}
    service._metrics_collectors = {}
    service._capability_registry = ConnectorCapabilityRegistry()
    service.db_manager = None

    connector = Connector()
    service._create_trading_connector = lambda account, name: connector

    def create_indices(connector_arg, account, name, cache_key):
        service._orders_recorders[cache_key] = StopIndex()
        service._funding_recorders[cache_key] = StopIndex()
        service._metrics_collectors[cache_key] = MetricIndex()

    service._initialize_metrics = create_indices

    async def fail_after_task(connector_arg):
        task = asyncio.create_task(blocker.wait())
        connector_arg._user_stream_tracker_task = task
        spawned.append(task)
        raise RuntimeError("network start failed")

    service._start_connector_network = fail_after_task
    with pytest.raises(RuntimeError):
        await service.get_trading_connector("acct", "generic")

    CHECK.assertEqual({}, service._trading_connectors)
    CHECK.assertEqual({}, service._connector_locks)
    CHECK.assertEqual({}, service._orders_recorders)
    CHECK.assertEqual({}, service._funding_recorders)
    CHECK.assertEqual({}, service._metrics_collectors)
    CHECK.assertEqual(1, len(spawned))
    CHECK.assertTrue(spawned[0].done())
    CHECK.assertIsNone(connector._user_stream_tracker_task)

    # Cancellation preserves the caller's cancellation while restoring exact
    # pre-existing per-key indices and draining a task spawned before cancellation.
    cache_key = "acct:generic"
    order_sentinel = object()
    funding_sentinel = object()
    metric_sentinel = object()
    service._orders_recorders[cache_key] = order_sentinel
    service._funding_recorders[cache_key] = funding_sentinel
    service._metrics_collectors[cache_key] = metric_sentinel
    connector = Connector()
    service._create_trading_connector = lambda account, name: connector
    replacement_indices = []

    def create_replacement_indices(connector_arg, account, name, key):
        orders = StopIndex()
        funding = StopIndex()
        metrics = MetricIndex()
        replacement_indices.extend((orders, funding, metrics))
        service._orders_recorders[key] = orders
        service._funding_recorders[key] = funding
        service._metrics_collectors[key] = metrics

    service._initialize_metrics = create_replacement_indices
    entered = asyncio.Event()
    cancelled_spawned = []

    async def wait_after_task(connector_arg):
        task = asyncio.create_task(blocker.wait())
        connector_arg._user_stream_tracker_task = task
        cancelled_spawned.append(task)
        entered.set()
        await asyncio.Event().wait()

    service._start_connector_network = wait_after_task
    creation = asyncio.create_task(service.get_trading_connector("acct", "generic"))
    await entered.wait()
    creation.cancel()
    with pytest.raises(asyncio.CancelledError):
        await creation
    CHECK.assertEqual({}, service._trading_connectors)
    CHECK.assertEqual({}, service._connector_locks)
    CHECK.assertIs(order_sentinel, service._orders_recorders[cache_key])
    CHECK.assertIs(funding_sentinel, service._funding_recorders[cache_key])
    CHECK.assertIs(metric_sentinel, service._metrics_collectors[cache_key])
    CHECK.assertTrue(all(index.stopped for index in replacement_indices))
    CHECK.assertEqual(1, len(cancelled_spawned))
    CHECK.assertTrue(cancelled_spawned[0].done())
    CHECK.assertIsNone(connector._user_stream_tracker_task)

    # Even a constructor seam that mutates the account map and spawns a task
    # before raising is owned by this creation transaction and is removed.
    partial_connector = Connector()
    partial_spawned = []

    def partially_mutating_create(account, name):
        service._trading_connectors.setdefault(account, {})[name] = partial_connector
        task = asyncio.create_task(blocker.wait())
        partial_connector._user_stream_tracker_task = task
        partial_spawned.append(task)
        raise RuntimeError("partial constructor failure")

    service._create_trading_connector = partially_mutating_create
    with pytest.raises(RuntimeError, match="partial constructor failure"):
        await service.get_trading_connector("partial", "generic")
    CHECK.assertNotIn("partial", service._trading_connectors)
    CHECK.assertEqual({}, service._connector_locks)
    CHECK.assertEqual(1, len(partial_spawned))
    CHECK.assertTrue(partial_spawned[0].done())
    CHECK.assertIsNone(partial_connector._user_stream_tracker_task)

    # A failed public network stop is an explicit residual command. A retry is
    # rejected before constructing again, and an empty later stop cannot invent success.
    class UncleanConnector(Connector):
        async def stop_network(self):
            return False

    unclean_connector = UncleanConnector()
    constructor_calls = 0

    def create_unclean(account, name):
        nonlocal constructor_calls
        constructor_calls += 1
        return unclean_connector

    service._create_trading_connector = create_unclean
    service._initialize_metrics = lambda *args: None

    async def fail_unclean_start(connector_arg):
        raise RuntimeError("unclean start failure")

    service._start_connector_network = fail_unclean_start
    with pytest.raises(module.ConnectorCapabilityError) as cleanup_residual:
        await service.get_trading_connector("dirty", "generic")
    CHECK.assertEqual(
        "connector_creation_cleanup_residual",
        cleanup_residual.value.detail["code"],
    )
    CHECK.assertIn("dirty:generic", service.connector_creation_residuals)
    with pytest.raises(module.ConnectorCapabilityError):
        await service.get_trading_connector("dirty", "generic")
    CHECK.assertEqual(1, constructor_calls)
    with pytest.raises(module.ConnectorCapabilityError):
        await service.stop_trading_connector("dirty", "generic")
    CHECK.assertIn("dirty:generic", service.connector_creation_residuals)


@pytest.mark.asyncio
async def test_final_b7_stale_pair_marker_rejects_before_remote_work():
    from services.connector_capabilities import ConnectorCapabilityError
    from services.trading_service import AccountTradingInterface

    tracker = TransactionTracker("BTC-USD")
    connector = TransactionConnector(tracker, "BTC-USD")
    connector_service = TransactionConnectorService(connector)
    initialize_calls = 0
    cleanup_calls = 0

    async def initialize_order_book(**kwargs):
        nonlocal initialize_calls
        initialize_calls += 1
        return False

    async def remove_trading_pair(**kwargs):
        nonlocal cleanup_calls
        cleanup_calls += 1
        return True

    interface = AccountTradingInterface(
        connector_service,
        types.SimpleNamespace(
            initialize_order_book=initialize_order_book,
            remove_trading_pair=remove_trading_pair,
        ),
        "master",
    )
    interface._markets = {"generic": {"BTC-USD"}}
    with pytest.raises(ConnectorCapabilityError) as error:
        await interface.add_market("generic", "BTC-USD")
    CHECK.assertEqual("ambiguous_market_state", error.value.detail["code"])
    CHECK.assertEqual(0, initialize_calls)
    CHECK.assertEqual(0, cleanup_calls)
    CHECK.assertEqual(["BTC-USD"], connector._trading_pairs)
    CHECK.assertEqual(["BTC-USD"], tracker._trading_pairs)


def test_final_b8_fixture_restores_preexisting_module_dicts():
    selected = ("deps", "models", "services")
    original_modules = {name: sys.modules.get(name) for name in selected}
    sentinels = {}
    before_dicts = {}
    try:
        for name in selected:
            module = types.ModuleType(name)
            module.__path__ = [str(Path(__file__).resolve().parents[1] / name)] if name != "deps" else []
            sentinel = object()
            module.preloaded_sentinel = sentinel
            if name == "deps":
                module.get_accounts_service = sentinel
            sentinels[name] = module
            before_dicts[name] = dict(module.__dict__)
            sys.modules[name] = module

        request = types.SimpleNamespace(node=types.SimpleNamespace(name="manual-fixture-check"))
        fixture = _scoped_import_contract.__wrapped__(request)
        next(fixture)
        CHECK.assertIsNot(sentinels["deps"].get_accounts_service, before_dicts["deps"]["get_accounts_service"])
        with pytest.raises(StopIteration):
            next(fixture)

        for name in selected:
            CHECK.assertIs(sentinels[name], sys.modules[name])
            CHECK.assertEqual(set(before_dicts[name]), set(sentinels[name].__dict__))
            for key, value in before_dicts[name].items():
                CHECK.assertIs(value, sentinels[name].__dict__[key], (name, key))
        polluted = [
            name
            for name, module in sys.modules.items()
            if getattr(module, "__issue222_contract_shim__", False)
        ]
        CHECK.assertEqual([], polluted)
        tree = ast.parse(Path(__file__).read_text(encoding="utf-8"))
        CHECK.assertEqual([], [node.lineno for node in ast.walk(tree) if isinstance(node, ast.Assert)])
    finally:
        for name, module in original_modules.items():
            if module is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = module
    tree = ast.parse(Path(__file__).read_text(encoding="utf-8"))
    bare_asserts = [node.lineno for node in ast.walk(tree) if isinstance(node, ast.Assert)]
    CHECK.assertEqual([], bare_asserts)


def test_closure_b1_advertised_trading_400_matches_runtime_body():
    from fastapi import FastAPI, HTTPException
    from fastapi.testclient import TestClient

    from models.connectors import CapabilityErrorResponse
    from routers import trading as trading_router

    class Accounts:
        def require_capability(self, *args, **kwargs):
            return None

        async def place_trade(self, **kwargs):
            raise HTTPException(
                status_code=400,
                detail="Price is required for LIMIT and LIMIT_MAKER orders",
            )

    app = FastAPI()
    app.include_router(trading_router.router)
    app.dependency_overrides[trading_router.get_accounts_service] = Accounts
    response = TestClient(app).post(
        "/trading/orders",
        json={
            "account_name": "master",
            "connector_name": "generic",
            "trading_pair": "BTC-USD",
            "trade_type": "BUY",
            "amount": "1",
            "order_type": "LIMIT",
            "position_action": "OPEN",
        },
    )

    CHECK.assertEqual(400, response.status_code)
    CapabilityErrorResponse.model_validate(response.json())


@pytest.mark.asyncio
async def test_closure_b2_public_executor_paths_reject_before_connector_work():
    from hummingbot.core.data_type.common import OrderType
    from models.connectors import CapabilityErrorDetail
    from services.connector_capabilities import ConnectorCapabilityError
    from services.trading_service import AccountTradingInterface

    calls = []

    class Connector:
        in_flight_orders = {"live": object()}

        def buy(self, **kwargs):
            calls.append("buy")
            return "buy-id"

        def sell(self, **kwargs):
            calls.append("sell")
            return "sell-id"

        def cancel(self, **kwargs):
            calls.append("cancel")
            return "cancel-id"

    connector = Connector()
    connector_service = types.SimpleNamespace(
        get_account_connectors=lambda account: {"manzana": connector}
    )

    async def remove_trading_pair(**kwargs):
        raise ConnectorCapabilityError(
            CapabilityErrorDetail(
                code="unsupported_connector_feature",
                message="not dynamic",
                connector="manzana",
                feature="dynamic_subscription",
            )
        )

    interface = AccountTradingInterface(
        connector_service,
        types.SimpleNamespace(remove_trading_pair=remove_trading_pair),
        "master",
    )
    for operation in (
        lambda: interface.buy("manzana", "BTC-USD", Decimal("1"), OrderType.LIMIT),
        lambda: interface.sell("manzana", "BTC-USD", Decimal("1"), OrderType.LIMIT),
        lambda: interface.cancel("manzana", "BTC-USD", "order-id"),
        lambda: interface.get_active_orders("manzana"),
    ):
        with pytest.raises(ConnectorCapabilityError):
            operation()
    interface._markets = {"manzana": {"BTC-USD"}}
    removal = await interface.remove_market("manzana", "BTC-USD")

    CHECK.assertEqual([], calls)
    CHECK.assertEqual("unsupported_connector_feature", removal.code)
    CHECK.assertEqual({"manzana": {"BTC-USD"}}, interface.markets)


@pytest.mark.asyncio
async def test_closure_b3_public_service_seams_gate_before_resolution():
    from services.connector_capabilities import ConnectorCapabilityError
    from services.market_data_service import MarketDataService

    calls = []

    class ConnectorService:
        def get_order_book_tracker_diagnostics(self, **kwargs):
            calls.append("diagnostics")
            return {"reached": True}

        async def restart_order_book_tracker(self, **kwargs):
            calls.append("restart")
            return {"reached": True}

    market_data = object.__new__(MarketDataService)
    market_data._connector_service = ConnectorService()
    with pytest.raises(ConnectorCapabilityError):
        market_data.get_order_book_tracker_diagnostics("manzana")
    with pytest.raises(ConnectorCapabilityError):
        await market_data.restart_order_book_tracker("manzana")

    module, registry_type = _load_unified_service_module()
    unified = object.__new__(module.UnifiedConnectorService)
    unified._capability_registry = registry_type()

    class StaticConnector:
        order_book_tracker = None

    def resolve(*args, **kwargs):
        calls.append("resolve")
        return StaticConnector()

    unified.get_best_connector_for_market = resolve
    with pytest.raises(module.ConnectorCapabilityError) as initialize_error:
        await unified.initialize_order_book("manzana", "BTC-USD")
    CHECK.assertEqual("dynamic_subscription", initialize_error.value.detail["feature"])
    with pytest.raises(module.ConnectorCapabilityError):
        await unified.remove_trading_pair("manzana", "BTC-USD")

    CHECK.assertEqual([], calls)


def _load_accounts_service_module():
    config = _module("config")
    config.settings = types.SimpleNamespace(banned_tokens=set())
    database = _module("database")
    database.AccountRepository = type("AccountRepository", (), {})
    database.AsyncDatabaseManager = type("AsyncDatabaseManager", (), {})
    config_crypt = _module("hummingbot.client.config.config_crypt")
    config_crypt.ETHKeyFileSecretManger = type("ETHKeyFileSecretManger", (), {})
    gateway_client = _module("services.gateway_client")
    gateway_client.GatewayClient = type("GatewayClient", (), {})
    transaction_poller = _module("services.gateway_transaction_poller")
    transaction_poller.GatewayTransactionPoller = type("GatewayTransactionPoller", (), {})
    wallet_service = _module("services.gateway_wallet_service")
    wallet_service.GatewayWalletService = type("GatewayWalletService", (), {})
    wallet_service.balance_entry = lambda token, units, price, available_units=None: {
        "token": token,
        "units": units,
        "price": price,
        "available_units": available_units,
    }
    perpetual_service = _module("services.perpetual_trading_service")
    perpetual_service.PerpetualTradingService = type("PerpetualTradingService", (), {})
    portfolio_service = _module("services.portfolio_analytics_service")
    portfolio_service.PortfolioAnalyticsService = type("PortfolioAnalyticsService", (), {})
    file_system = _module("utils.file_system")
    file_system.fs_util = types.SimpleNamespace()
    gateway_certs = _module("utils.gateway_certs")
    gateway_certs.build_client_ssl_context = lambda *args, **kwargs: None

    module_name = "issue222_closure_accounts_service_under_test"
    spec = importlib.util.spec_from_file_location(
        module_name,
        Path(__file__).resolve().parents[1] / "services" / "accounts_service.py",
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.asyncio
async def test_closure_b2_accounts_active_orders_gate_before_connector_lookup():
    from services.connector_capabilities import (
        ConnectorCapabilityError,
        ConnectorCapabilityRegistry,
    )

    module = _load_accounts_service_module()
    connector_calls = []

    class ConnectorService:
        registry = ConnectorCapabilityRegistry()

        def require_capability(self, connector_name, feature, trading_pair=None):
            self.registry.require(connector_name, feature, trading_pair)

        async def get_trading_connector(self, account_name, connector_name):
            connector_calls.append((account_name, connector_name))
            return types.SimpleNamespace(in_flight_orders={})

    service = object.__new__(module.AccountsService)
    service._connector_service = ConnectorService()
    service.list_accounts = lambda: ["master"]

    with pytest.raises(ConnectorCapabilityError):
        await service.get_active_orders("master", "manzana")
    CHECK.assertEqual([], connector_calls)


@pytest.mark.asyncio
async def test_closure_b4_last_trade_paths_gate_before_balance_or_network_work():
    from services.connector_capabilities import (
        ConnectorCapabilityError,
        ConnectorCapabilityRegistry,
    )

    accounts_module = _load_accounts_service_module()
    balance_calls = []

    class Connector:
        async def _update_balances(self):
            balance_calls.append("balances")

        def get_all_balances(self):
            return {"BTC": Decimal("1")}

        def get_available_balance(self, token):
            return Decimal("1")

    connector = Connector()

    class ConnectorService:
        registry = ConnectorCapabilityRegistry()

        def get_all_trading_connectors(self):
            return {"master": {"manzana": connector}}

        def is_gateway_connector(self, connector_arg):
            return False

        def require_capability(self, connector_name, feature, trading_pair=None):
            self.registry.require(connector_name, feature, trading_pair)

    accounts = object.__new__(accounts_module.AccountsService)
    accounts._connector_service = ConnectorService()
    accounts._market_data_service = types.SimpleNamespace(
        get_rate_for_connector=lambda *args: None
    )
    accounts.accounts_state = {}
    accounts.default_quotes = {}
    accounts.default_quote = "USD"
    accounts._last_known_prices = {}

    with pytest.raises(ConnectorCapabilityError):
        await accounts.update_account_state(skip_gateway=True)
    CHECK.assertEqual([], balance_calls)

    config = _module("config")
    config.settings = types.SimpleNamespace()
    ticker_name = "issue222_closure_ticker_sources_under_test"
    ticker_spec = importlib.util.spec_from_file_location(
        ticker_name,
        Path(__file__).resolve().parents[1] / "services" / "ticker_sources.py",
    )
    ticker_module = importlib.util.module_from_spec(ticker_spec)
    ticker_spec.loader.exec_module(ticker_module)
    ticker_calls = []

    class TickerConnector:
        async def trading_pair_symbol_map(self):
            ticker_calls.append("symbol_map")
            return {"BTCUSD": "BTC-USD"}

    with pytest.raises(ConnectorCapabilityError):
        await ticker_module._fetch(TickerConnector(), "manzana")
    CHECK.assertEqual([], ticker_calls)

    event_package = _module("hummingbot.core.event")
    event_package.__path__ = []
    event_forwarder = _module("hummingbot.core.event.event_forwarder")

    class SourceInfoEventForwarder:
        def __init__(self, callback):
            self.callback = callback

    event_forwarder.SourceInfoEventForwarder = SourceInfoEventForwarder
    events = _module("hummingbot.core.event.events")
    events.BuyOrderCreatedEvent = type("BuyOrderCreatedEvent", (), {})
    events.OrderFilledEvent = type("OrderFilledEvent", (), {})
    events.SellOrderCreatedEvent = type("SellOrderCreatedEvent", (), {})
    events.MarketEvent = Enum(
        "MarketEvent",
        "BuyOrderCreated SellOrderCreated OrderFilled OrderCancelled OrderFailure "
        "BuyOrderCompleted SellOrderCompleted",
    )
    events.TradeType = Enum("TradeType", "BUY SELL")
    database = _module("database")
    database.AsyncDatabaseManager = type("AsyncDatabaseManager", (), {})
    database.OrderRepository = type("OrderRepository", (), {})
    database.TradeRepository = type("TradeRepository", (), {})
    recorder_name = "issue222_closure_orders_recorder_under_test"
    recorder_spec = importlib.util.spec_from_file_location(
        recorder_name,
        Path(__file__).resolve().parents[1] / "services" / "orders_recorder.py",
    )
    recorder_module = importlib.util.module_from_spec(recorder_spec)
    recorder_spec.loader.exec_module(recorder_module)
    recorder = recorder_module.OrdersRecorder(None, "master", "manzana")
    recorder_calls = []

    class RecorderConnector:
        async def _get_last_traded_price(self, trading_pair):
            recorder_calls.append(trading_pair)
            return Decimal("1")

    recorder._connector = RecorderConnector()
    with pytest.raises(ConnectorCapabilityError):
        await recorder._fetch_conversion_rate("BTC", "USD")
    CHECK.assertEqual([], recorder_calls)


@pytest.mark.asyncio
async def test_closure_b5_connector_scope_residual_cannot_erase_another_pair():
    from services.connector_capabilities import ConnectorCapabilityError
    from services.trading_service import AccountTradingInterface

    class ReadyBook:
        snapshot = ([1], [1])

    tracker = TransactionTracker()
    connector = TransactionConnector(tracker)

    class ConnectorService:
        loaded = False
        cleanup_succeeds = False

        def get_account_connectors(self, account):
            return {"generic": connector} if self.loaded else {}

        async def get_trading_connector(self, account, name):
            self.loaded = True
            return connector

        async def stop_trading_connector(self, account, name):
            if not self.cleanup_succeeds:
                return False
            connector._trading_pairs.clear()
            tracker._trading_pairs.clear()
            tracker.order_books.clear()
            self.loaded = False
            return True

    connector_service = ConnectorService()
    a_attempts = 0

    async def initialize_order_book(**kwargs):
        nonlocal a_attempts
        pair = kwargs["trading_pair"]
        connector._trading_pairs.append(pair)
        tracker._trading_pairs.append(pair)
        tracker.order_books[pair] = ReadyBook()
        if pair == "A-USD":
            a_attempts += 1
            return a_attempts > 1
        return True

    interface = AccountTradingInterface(
        connector_service,
        types.SimpleNamespace(initialize_order_book=initialize_order_book),
        "master",
    )
    with pytest.raises(ConnectorCapabilityError):
        await interface.add_market("generic", "A-USD")
    peer = AccountTradingInterface(
        connector_service,
        types.SimpleNamespace(initialize_order_book=initialize_order_book),
        "master",
    )
    CHECK.assertIn(("generic", "A-USD"), peer.cleanup_residuals)

    # A connector-owned residual blocks every pair and every interface sharing
    # this account/connector authority. It must reject before initializing B.
    with pytest.raises(ConnectorCapabilityError):
        await peer.add_market("generic", "B-USD")
    CHECK.assertNotIn("B-USD", tracker.order_books)

    # Once the retained cleanup can complete, a peer retry may consume A's
    # connector residual and then publish B without leaving stale ownership.
    connector_service.cleanup_succeeds = True
    await peer.add_market("generic", "B-USD")
    await interface.add_market("generic", "A-USD")

    CHECK.assertEqual({"A-USD", "B-USD"}, set(connector._trading_pairs))
    CHECK.assertEqual({"A-USD", "B-USD"}, set(tracker.order_books))


@pytest.mark.asyncio
async def test_closure_b7_pair_task_cleanup_is_hard_bounded_and_residual():
    from services.trading_service import AccountTradingInterface

    tracker = TransactionTracker()
    connector = TransactionConnector(tracker)
    connector_service = TransactionConnectorService(connector)
    release = asyncio.Event()
    resistant_started = asyncio.Event()

    async def resistant():
        resistant_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await release.wait()

    async def initialize_order_book(**kwargs):
        task = asyncio.create_task(resistant())
        tracker._tracking_tasks["BTC-USD"] = task
        await resistant_started.wait()
        return False

    interface = AccountTradingInterface(
        connector_service,
        types.SimpleNamespace(
            initialize_order_book=initialize_order_book,
            remove_trading_pair=lambda **kwargs: asyncio.sleep(0, result=True),
        ),
        "master",
    )
    add_task = asyncio.create_task(interface.add_market("generic", "BTC-USD"))
    try:
        await asyncio.sleep(0.05)
        CHECK.assertTrue(add_task.done())
        CHECK.assertIn(("generic", "BTC-USD"), interface.cleanup_residuals)
    finally:
        release.set()
        await asyncio.gather(add_task, return_exceptions=True)


@pytest.mark.asyncio
async def test_closure_b8_repeated_cancellation_prunes_market_locks():
    from services.trading_service import AccountTradingInterface

    interface = AccountTradingInterface(
        types.SimpleNamespace(get_account_connectors=lambda account: {}),
        types.SimpleNamespace(),
        "master",
    )
    entered = asyncio.Event()
    leave_body = asyncio.Event()

    async def transaction():
        async with interface._market_transaction("generic"):
            entered.set()
            await leave_body.wait()

    task = asyncio.create_task(transaction())
    await entered.wait()
    await interface._market_locks_guard.acquire()
    leave_body.set()
    await asyncio.sleep(0)
    task.cancel()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    interface._market_locks_guard.release()

    CHECK.assertEqual({}, interface._market_lock_users)
    CHECK.assertEqual({}, interface._market_locks)


@pytest.mark.asyncio
async def test_closure_b8_late_cleanup_cancellation_propagates():
    from services.trading_service import AccountTradingInterface

    tracker = TransactionTracker()
    connector = TransactionConnector(tracker)
    connector_service = TransactionConnectorService(connector)
    cleanup_entered = asyncio.Event()
    cleanup_release = asyncio.Event()

    async def remove_trading_pair(**kwargs):
        cleanup_entered.set()
        await cleanup_release.wait()
        return True

    interface = AccountTradingInterface(
        connector_service,
        types.SimpleNamespace(
            initialize_order_book=lambda **kwargs: asyncio.sleep(0, result=False),
            remove_trading_pair=remove_trading_pair,
        ),
        "master",
    )
    task = asyncio.create_task(interface.add_market("generic", "BTC-USD"))
    await cleanup_entered.wait()
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    cleanup_release.set()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_closure_b6_retry_verifies_local_cleanup_and_typed_scope():
    from services.connector_capabilities import ConnectorCapabilityError
    from services.trading_service import AccountTradingInterface

    tracker = TransactionTracker()
    connector = TransactionConnector(tracker)
    connector_service = TransactionConnectorService(connector)

    async def initialize_order_book(**kwargs):
        tracker.order_books["BTC-USD"] = object()
        return False

    interface = AccountTradingInterface(
        connector_service,
        types.SimpleNamespace(
            initialize_order_book=initialize_order_book,
            remove_trading_pair=lambda **kwargs: asyncio.sleep(0, result=True),
        ),
        "master",
    )

    async def fail_local_cleanup(*args, **kwargs):
        raise RuntimeError("sensitive raw cleanup text")

    interface._remove_owned_pair_state = fail_local_cleanup
    with pytest.raises(ConnectorCapabilityError) as first:
        await interface.add_market("generic", "BTC-USD")
    CHECK.assertEqual("pair", first.value.detail["cleanup_scope"])
    CHECK.assertIn("cleanup_code", first.value.detail)
    CHECK.assertNotIn("sensitive", json.dumps(first.value.detail))

    with pytest.raises(ConnectorCapabilityError):
        await interface.add_market("generic", "BTC-USD")
    CHECK.assertIn(("generic", "BTC-USD"), interface.cleanup_residuals)
    CHECK.assertIn("BTC-USD", tracker.order_books)


@pytest.mark.asyncio
async def test_closure_b9_real_tracker_task_names_are_drained_or_retained():
    module, registry_type = _load_unified_service_module()
    service = object.__new__(module.UnifiedConnectorService)
    service._capability_registry = registry_type()
    release = asyncio.Event()

    async def resistant():
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await release.wait()

    tasks = [asyncio.create_task(resistant()), asyncio.create_task(resistant())]
    await asyncio.sleep(0)

    class Tracker:
        _tracking_tasks = {}
        _init_order_books_task = tasks[0]
        _emit_trade_event_task = tasks[1]

        def stop(self):
            for task in tasks:
                task.cancel()
            self._init_order_books_task = None
            self._emit_trade_event_task = None

    connector = types.SimpleNamespace(order_book_tracker=Tracker())

    try:
        with pytest.raises(RuntimeError) as captured:
            await service._stop_connector_network(connector)
        CHECK.assertEqual("task_cleanup_pending", str(captured.value))
        CHECK.assertTrue(all(task.cancelling() == 1 for task in tasks))
        CHECK.assertTrue(bool(service._connector_residual_tasks))

        with pytest.raises(RuntimeError):
            await service._stop_connector_network(connector)
        CHECK.assertTrue(all(task.cancelling() == 1 for task in tasks))
    finally:
        release.set()
        await asyncio.gather(*tasks, return_exceptions=True)

    await service._stop_connector_network(connector)
    CHECK.assertEqual({}, service._connector_residual_tasks)


@pytest.mark.asyncio
async def test_closure_b10_failed_creation_cleanup_false_is_not_repeated():
    module, registry_type = _load_unified_service_module()

    class Connector:
        order_book_tracker = None
        in_flight_orders = {}

        def __init__(self):
            self.clean = False
            self.stop_calls = 0

        async def _initialize_trading_pair_symbol_map(self):
            return None

        async def _update_trading_rules(self):
            return None

        async def _update_balances(self):
            return None

        async def stop_network(self):
            self.stop_calls += 1
            return self.clean

    service = object.__new__(module.UnifiedConnectorService)
    service._trading_connectors = {}
    service._connector_locks = {}
    service._connector_lock_users = {}
    service._connector_locks_guard = asyncio.Lock()
    service._orders_recorders = {}
    service._funding_recorders = {}
    service._metrics_collectors = {}
    service._connector_creation_residuals = {}
    service._capability_registry = registry_type()
    service.db_manager = None
    connector = Connector()
    service._create_trading_connector = lambda account, name: connector
    service._initialize_metrics = lambda *args: None

    async def fail_start(connector_arg):
        raise RuntimeError("start failed")

    service._start_connector_network = fail_start
    with pytest.raises(module.ConnectorCapabilityError):
        await service.get_trading_connector("master", "generic")
    connector.clean = True

    with pytest.raises(module.ConnectorCapabilityError) as retry:
        await service.stop_trading_connector("master", "generic")
    CHECK.assertEqual("connector_cleanup_residual", retry.value.detail["code"])
    CHECK.assertEqual(1, connector.stop_calls)
    CHECK.assertIn("master:generic", service.connector_creation_residuals)


@pytest.mark.asyncio
async def test_closure_b11_stop_serializes_with_same_key_creation():
    module, registry_type = _load_unified_service_module()

    class Index:
        async def stop(self):
            return None

    service = object.__new__(module.UnifiedConnectorService)
    service._trading_connectors = {}
    service._connector_locks = {}
    service._connector_lock_users = {}
    service._connector_locks_guard = asyncio.Lock()
    service._orders_recorders = {}
    service._funding_recorders = {}
    service._metrics_collectors = {}
    service._connector_creation_residuals = {}
    service._capability_registry = registry_type()
    service.db_manager = None
    creation_paused = asyncio.Event()
    creation_release = asyncio.Event()
    connector = types.SimpleNamespace()

    async def create(account, name):
        key = f"{account}:{name}"
        service._orders_recorders[key] = Index()
        service._funding_recorders[key] = Index()
        service._metrics_collectors[key] = Index()
        creation_paused.set()
        await creation_release.wait()
        return connector

    service._create_and_initialize_trading_connector = create
    creation = asyncio.create_task(service.get_trading_connector("master", "generic"))
    await creation_paused.wait()
    stopping = asyncio.create_task(service.stop_trading_connector("master", "generic"))
    try:
        await asyncio.wait({stopping}, timeout=0.1)
        CHECK.assertFalse(stopping.done())
    finally:
        creation_release.set()
        await asyncio.gather(creation, stopping)
    CHECK.assertIs(connector, creation.result())
    CHECK.assertIs(True, stopping.result())

    CHECK.assertEqual({}, service._trading_connectors)
    CHECK.assertEqual({}, service._orders_recorders)
    CHECK.assertEqual({}, service._funding_recorders)
    CHECK.assertEqual({}, service._metrics_collectors)


@pytest.mark.asyncio
async def test_residual_b2_connector_lock_queued_cancellation_propagates_and_prunes():
    module, _ = _load_unified_service_module()
    service = object.__new__(module.UnifiedConnectorService)
    service._connector_locks = {}
    service._connector_lock_users = {}
    service._connector_locks_guard = asyncio.Lock()
    entered = asyncio.Event()
    leave_body = asyncio.Event()

    async def transaction():
        async with service._connector_creation_lock(("master", "generic")):
            entered.set()
            await leave_body.wait()

    task = asyncio.create_task(transaction())
    await entered.wait()
    await service._connector_locks_guard.acquire()
    leave_body.set()
    await asyncio.sleep(0)
    task.cancel()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    service._connector_locks_guard.release()

    CHECK.assertEqual({}, service._connector_lock_users)
    CHECK.assertEqual({}, service._connector_locks)


@pytest.mark.asyncio
async def test_residual_b3_owner_tasks_are_consumed_before_public_clear():
    module, registry_type = _load_unified_service_module()
    release = asyncio.Event()

    class ResistantIndex:
        def __init__(self):
            self.calls = 0

        async def stop(self):
            self.calls += 1
            if self.calls > 1:
                return None
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await release.wait()

    service = object.__new__(module.UnifiedConnectorService)
    service._trading_connectors = {}
    service._orders_recorders = {"master:generic": ResistantIndex()}
    service._funding_recorders = {}
    service._metrics_collectors = {}
    service._connector_cleanup_indices = {}
    service._connector_cleanup_owners = {}
    service._connector_creation_residuals = {}
    service._connector_residual_tasks = {}
    service._capability_registry = registry_type()

    first = await service._finalize_stop_trading_connector("master", "generic")
    CHECK.assertIsNotNone(first)
    CHECK.assertTrue(service._connector_residual_tasks)

    second = await service._finalize_stop_trading_connector("master", "generic")
    CHECK.assertIsNotNone(second)
    CHECK.assertTrue(service._connector_creation_residuals)
    CHECK.assertTrue(service._connector_residual_tasks)

    pending = {
        task
        for tasks in service._connector_residual_tasks.values()
        for task in tasks
    }
    release.set()
    await asyncio.gather(*pending, return_exceptions=True)

    third = await service._finalize_stop_trading_connector("master", "generic")
    CHECK.assertIsNone(third)
    CHECK.assertEqual({}, service._orders_recorders)
    CHECK.assertEqual({}, service._connector_creation_residuals)
    CHECK.assertEqual({}, service._connector_residual_tasks)


@pytest.mark.asyncio
async def test_residual_b3_pair_task_retry_does_not_recancel_retained_owner():
    from services.trading_service import AccountTradingInterface

    release = asyncio.Event()
    cancelled_once = asyncio.Event()

    async def resistant():
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled_once.set()
            await release.wait()

    connector_service = types.SimpleNamespace(
        get_account_connectors=lambda account: {}
    )
    interface = AccountTradingInterface(
        connector_service, types.SimpleNamespace(), "master"
    )
    task = asyncio.create_task(resistant())
    await asyncio.sleep(0)
    key = ("master", "generic", "pair_tasks", "BTC-USD")

    CHECK.assertIs(False, await interface._drain_tasks(key, {task}))
    await cancelled_once.wait()
    CHECK.assertIs(False, await interface._drain_tasks(key, set()))
    CHECK.assertFalse(task.done())

    release.set()
    await task
    CHECK.assertIs(True, await interface._drain_tasks(key, set()))
    CHECK.assertEqual({}, interface._residual_tasks)


@pytest.mark.asyncio
async def test_residual_b4_account_cleanup_is_exact_and_interface_locks_are_shared():
    module, registry_type = _load_unified_service_module()
    from services.trading_service import AccountTradingInterface

    calls = []

    class Connector:
        async def remove_trading_pair(self, trading_pair):
            calls.append(trading_pair)
            return True

    service = object.__new__(module.UnifiedConnectorService)
    service._capability_registry = registry_type()
    service._trading_connectors = {
        "account-a": {},
        "account-b": {"generic": Connector()},
    }

    def forbidden_fallback(*args, **kwargs):
        raise AssertionError("account cleanup used cross-account fallback")

    service.get_best_connector_for_market = forbidden_fallback
    removed = await service.remove_trading_pair(
        "generic", "BTC-USD", account_name="account-a"
    )
    CHECK.assertIs(False, removed)
    CHECK.assertEqual([], calls)

    service.get_account_connectors = lambda account: {}
    market_data = types.SimpleNamespace()
    first = AccountTradingInterface(service, market_data, "account-a")
    second = AccountTradingInterface(service, market_data, "account-a")
    other_account = AccountTradingInterface(service, market_data, "account-b")
    CHECK.assertIs(first._market_locks, second._market_locks)
    CHECK.assertIs(first._market_locks_guard, second._market_locks_guard)
    CHECK.assertIs(first._market_locks, other_account._market_locks)


def _load_executor_service_module():
    strategy_v2 = _module("hummingbot.strategy_v2")
    strategy_v2.__path__ = []
    executors = _module("hummingbot.strategy_v2.executors")
    executors.__path__ = []

    class ExecutorBase:
        pass

    class ExecutorConfigBase(BaseModel):
        pass

    executor_base = _module("hummingbot.strategy_v2.executors.executor_base")
    executor_base.ExecutorBase = ExecutorBase
    executor_types = _module("hummingbot.strategy_v2.executors.data_types")
    executor_types.ExecutorConfigBase = ExecutorConfigBase

    registrations = (
        ("arbitrage_executor", "ArbitrageExecutor", "ArbitrageExecutorConfig"),
        ("dca_executor", "DCAExecutor", "DCAExecutorConfig"),
        ("grid_executor", "GridExecutor", "GridExecutorConfig"),
        ("lp_executor", "LPExecutor", "LPExecutorConfig"),
        ("order_executor", "OrderExecutor", "OrderExecutorConfig"),
        ("position_executor", "PositionExecutor", "PositionExecutorConfig"),
        ("twap_executor", "TWAPExecutor", "TWAPExecutorConfig"),
        ("xemm_executor", "XEMMExecutor", "XEMMExecutorConfig"),
    )
    for package, executor_name, config_name in registrations:
        package_module = _module(f"hummingbot.strategy_v2.executors.{package}")
        package_module.__path__ = []
        implementation = _module(
            f"hummingbot.strategy_v2.executors.{package}.{package}"
        )
        setattr(implementation, executor_name, type(executor_name, (ExecutorBase,), {}))
        data_types = _module(
            f"hummingbot.strategy_v2.executors.{package}.data_types"
        )
        setattr(data_types, config_name, type(config_name, (ExecutorConfigBase,), {}))

    executor_models = _module("hummingbot.strategy_v2.models")
    executor_models.__path__ = []
    executor_model_types = _module("hummingbot.strategy_v2.models.executors")
    executor_model_types.CloseType = Enum("CloseType", "COMPLETED")
    executor_model_types.TrackedOrder = type("TrackedOrder", (), {})

    database = _module("database")
    for name in (
        "AsyncDatabaseManager",
        "ExecutorPerformanceRepository",
        "ExecutorRepository",
        "GatewayCLMMRepository",
        "GatewaySwapRepository",
    ):
        setattr(database, name, type(name, (), {}))

    executor_models_module = _module("models.executors")
    executor_models_module.PositionHold = type("PositionHold", (), {})
    gateway_client = _module("services.gateway_client")
    gateway_client.get_native_gas_token = lambda *args, **kwargs: "ETH"

    log_capture = _module("utils.executor_log_capture")
    log_capture.ExecutorLogCapture = type("ExecutorLogCapture", (), {})

    class Context:
        def set(self, value):
            return value

        def reset(self, token):
            return None

    log_capture.current_executor_id = Context()

    module_name = "issue222_residual_executor_service_under_test"
    spec = importlib.util.spec_from_file_location(
        module_name,
        Path(__file__).resolve().parents[1] / "services" / "executor_service.py",
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_websocket_manager_module():
    event_package = _module("hummingbot.core.event")
    event_package.__path__ = []
    event_forwarder = _module("hummingbot.core.event.event_forwarder")
    event_forwarder.SourceInfoEventForwarder = type(
        "SourceInfoEventForwarder", (), {}
    )
    events = _module("hummingbot.core.event.events")
    events.OrderBookEvent = Enum("OrderBookEvent", "TradeEvent")
    events.OrderBookTradeEvent = type("OrderBookTradeEvent", (), {})

    config = _module("config")
    config.settings = types.SimpleNamespace(
        market_data=types.SimpleNamespace(
            candles_ready_timeout=1.0,
            ws_min_update_interval=0.1,
            ws_max_update_interval=5.0,
        )
    )

    module_name = "issue222_residual_websocket_manager_under_test"
    spec = importlib.util.spec_from_file_location(
        module_name,
        Path(__file__).resolve().parents[1] / "services" / "websocket_manager.py",
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.asyncio
async def test_residual_b5_stop_all_retries_unpublished_owner_union():
    module, registry_type = _load_unified_service_module()

    class Connector:
        order_book_tracker = None

        def __init__(self):
            self.stop_calls = 0

        async def stop_network(self):
            self.stop_calls += 1
            return None

    connector = Connector()
    market_connector = Connector()
    service = object.__new__(module.UnifiedConnectorService)
    service._trading_connectors = {}
    service._data_connectors = {}
    service._data_connectors_started = {}
    service._connector_locks = {}
    service._connector_lock_users = {}
    service._connector_locks_guard = asyncio.Lock()
    service._orders_recorders = {}
    service._funding_recorders = {}
    service._metrics_collectors = {}
    service._connector_creation_residuals = {
        ("master", "generic"): {
            "code": "connector_creation_cleanup_residual",
            "message": "cleanup incomplete",
            "connector": "generic",
            "feature": "connector_creation",
            "reason": "cleanup_incomplete",
            "cleanup_scope": "connector",
            "cleanup_code": "connector_cleanup_incomplete",
        }
    }
    service._connector_cleanup_owners = {
        ("master", "generic", "network"): connector
    }
    service._connector_cleanup_indices = {}
    service._connector_residual_tasks = {}
    service._market_cleanup_residuals = {}
    service._market_cleanup_owners = {
        ("secondary", "orphan", "connector"): {
            "connector": market_connector,
            "connector_preexisting": False,
        }
    }
    service._market_residual_tasks = {}
    service._market_pair_owners = {}
    service._capability_registry = registry_type()

    await service.stop_all()

    CHECK.assertEqual(1, connector.stop_calls)
    CHECK.assertEqual(1, market_connector.stop_calls)
    CHECK.assertEqual({}, service._connector_creation_residuals)
    CHECK.assertEqual({}, service._connector_cleanup_owners)
    CHECK.assertEqual({}, service._market_cleanup_owners)


@pytest.mark.asyncio
async def test_residual_b6_ticker_truth_rejects_before_local_or_connector_work():
    from services.connector_capabilities import (
        ConnectorCapabilityError,
        ConnectorCapabilityRegistry,
        ConnectorFeature,
    )
    from services.market_data_service import MarketDataService

    calls = []

    class ConnectorService:
        registry = ConnectorCapabilityRegistry()

        def require_capability(self, connector_name, feature, trading_pair=None):
            self.registry.require(connector_name, feature, trading_pair)

        def is_known_connector(self, connector_name):
            calls.append("known")
            return True

        def get_best_connector_for_market(self, connector_name):
            calls.append("resolve")
            return object()

    service = MarketDataService(ConnectorService())
    capabilities = ConnectorCapabilityRegistry().get("manzana")
    CHECK.assertNotIn(ConnectorFeature.TICKERS.value, capabilities.features)
    CHECK.assertEqual("best_bid_ask", ConnectorFeature.BEST_BID_ASK.value)

    with pytest.raises(ConnectorCapabilityError) as captured:
        await service.fetch_connector_tickers("manzana", force=True)

    CHECK.assertEqual(ConnectorFeature.TICKERS.value, captured.value.detail["feature"])
    CHECK.assertEqual([], calls)
    CHECK.assertNotIn("manzana", service._ticker_requests)
    CHECK.assertNotIn("manzana", service._ticker_locks)

    tickers, errors = await service.fetch_tickers_for(["manzana"], force=True)
    CHECK.assertEqual({}, tickers)
    CHECK.assertIn("manzana", errors)
    CHECK.assertEqual([], calls)


@pytest.mark.asyncio
async def test_residual_b7_account_refresh_rejects_before_all_state_and_network_work():
    from services.connector_capabilities import (
        ConnectorCapabilityError,
        ConnectorCapabilityRegistry,
    )

    module = _load_accounts_service_module()
    calls = []

    class ConnectorService:
        registry = ConnectorCapabilityRegistry()

        def get_all_trading_connectors(self):
            return {"master": {"manzana": object()}}

        def is_gateway_connector(self, connector):
            return False

        def require_capability(self, connector_name, feature, trading_pair=None):
            self.registry.require(connector_name, feature, trading_pair)

        async def refresh_connector_state(self, *args):
            calls.append("refresh")

    accounts = object.__new__(module.AccountsService)
    accounts._connector_service = ConnectorService()
    accounts.accounts_state = {}
    accounts._update_gateway_balances = lambda **kwargs: asyncio.sleep(0)

    with pytest.raises(ConnectorCapabilityError):
        await accounts.update_account_state(skip_gateway=True)
    CHECK.assertEqual({}, accounts.accounts_state)

    with pytest.raises(ConnectorCapabilityError):
        await accounts._refresh_and_get_tokens_info(
            object(), "manzana", "master"
        )
    CHECK.assertEqual([], calls)


@pytest.mark.asyncio
async def test_residual_b7_nested_executor_configs_reject_before_interface_or_config_mutation():
    from services.connector_capabilities import (
        ConnectorCapabilityError,
        ConnectorCapabilityRegistry,
    )

    module = _load_executor_service_module()
    calls = []

    class ConnectorService:
        registry = ConnectorCapabilityRegistry()

        def require_capability(self, connector_name, feature, trading_pair=None):
            self.registry.require(connector_name, feature, trading_pair)

    class TradingService:
        connector_service = ConnectorService()

        def get_trading_interface(self, account):
            calls.append(("interface", account))
            return types.SimpleNamespace(current_timestamp=123.0)

    service = object.__new__(module.ExecutorService)
    service._trading_service = TradingService()
    service.default_account = "master"
    service._trading_interfaces = {}

    def forbidden_validation(*args, **kwargs):
        calls.append(("validate", None))
        raise AssertionError("config validation preceded capability rejection")

    service._validate_executor_config = forbidden_validation
    cases = (
        {
            "type": "arbitrage_executor",
            "buying_market": {
                "connector_name": "manzana",
                "trading_pair": "BTC-USD",
            },
            "selling_market": {
                "connector_name": "generic",
                "trading_pair": "BTC-USD",
            },
        },
        {
            "type": "xemm_executor",
            "maker_market": {
                "connector_name": "generic",
                "trading_pair": "BTC-USD",
            },
            "taker_market": {
                "connector_name": "manzana",
                "trading_pair": "BTC-USD",
            },
        },
    )
    for config in cases:
        original = json.loads(json.dumps(config))
        with pytest.raises(ConnectorCapabilityError):
            await service.create_executor(config, account_name="master")
        CHECK.assertEqual(original, config)
    CHECK.assertEqual([], calls)
    CHECK.assertEqual({}, service._trading_interfaces)


@pytest.mark.asyncio
async def test_residual_b7_websocket_rejection_preserves_existing_subscription():
    from services.connector_capabilities import (
        ConnectorCapabilityRegistry,
    )

    module = _load_websocket_manager_module()
    calls = []

    class Task:
        cancelled = False

        def done(self):
            return False

        def cancel(self):
            self.cancelled = True

    old_task = Task()
    old = module.Subscription(
        subscription_id="order_book_manzana_BTC-USD",
        sub_type="order_book",
        connector="generic",
        trading_pair="BTC-USD",
        update_interval=1.0,
        task=old_task,
    )

    class MarketData:
        registry = ConnectorCapabilityRegistry()

        def require_capability(self, connector_name, feature, trading_pair=None):
            self.registry.require(connector_name, feature, trading_pair)

        async def initialize_order_book(self, connector_name, trading_pair):
            calls.append("initialize")
            self.require_capability(connector_name, feature=None, trading_pair=trading_pair)

    class WebSocket:
        async def send_json(self, payload):
            calls.append(payload)

    manager = module.WebSocketManager(MarketData())
    manager._connections = {"connection": {old.subscription_id: old}}
    await manager.handle_subscribe(
        "connection",
        WebSocket(),
        {
            "type": "order_book",
            "connector": "manzana",
            "trading_pair": "BTC-USD",
        },
    )

    CHECK.assertIs(
        old, manager._connections["connection"][old.subscription_id]
    )
    CHECK.assertFalse(old_task.cancelled)
    CHECK.assertNotIn("initialize", calls)


@pytest.mark.asyncio
async def test_residual_b8_public_capability_details_never_publish_exception_text():
    from services.connector_capabilities import ConnectorCapabilityError
    from routers import market_data as market_data_router

    secret = "transport-token=credential-value"

    class Service:
        def require_capability(self, *args, **kwargs):
            return None

        async def get_candles_feed(self, config):
            raise ValueError(secret)

    request = types.SimpleNamespace(
        app=types.SimpleNamespace(
            state=types.SimpleNamespace(market_data_service=Service())
        )
    )
    config = market_data_router.CandlesConfigRequest(
        connector_name="generic",
        trading_pair="BTC-USD",
        interval="1m",
        max_records=10,
    )
    market_data_router.CandlesFactory._candles_map["generic"] = object()
    try:
        with pytest.raises(ConnectorCapabilityError) as captured:
            await market_data_router.get_candles(request, config)
    finally:
        market_data_router.CandlesFactory._candles_map.pop("generic", None)

    serialized = json.dumps(captured.value.detail)
    CHECK.assertNotIn(secret, serialized)
    CHECK.assertNotIn("ValueError", serialized)
    CHECK.assertEqual("candle_request_rejected", captured.value.detail["reason"])


def _adjacent_unified_service(module, registry_type):
    service = object.__new__(module.UnifiedConnectorService)
    service._trading_connectors = {}
    service._data_connectors = {}
    service._data_connectors_started = {}
    service._connector_locks = {}
    service._connector_lock_users = {}
    service._connector_locks_guard = asyncio.Lock()
    service._orders_recorders = {}
    service._funding_recorders = {}
    service._metrics_collectors = {}
    service._connector_creation_residuals = {}
    service._connector_cleanup_owners = {}
    service._connector_cleanup_indices = {}
    service._connector_residual_tasks = {}
    service._market_cleanup_residuals = {}
    service._market_cleanup_owners = {}
    service._market_residual_tasks = {}
    service._market_pair_owners = {}
    service._capability_registry = registry_type()
    service.db_manager = None
    return service


@pytest.mark.asyncio
async def test_adjacent_b1_same_pair_peer_release_is_reference_safe():
    from services.trading_service import AccountTradingInterface

    class ReadyBook:
        snapshot = ([1], [1])

    tracker = TransactionTracker()
    connector = TransactionConnector(tracker)
    connector._trading_pairs[:] = ["BTC-USD"]
    tracker._trading_pairs[:] = ["BTC-USD"]
    tracker.order_books["BTC-USD"] = ReadyBook()

    class ConnectorService:
        def get_account_connectors(self, account):
            return {"generic": connector}

    removed = []

    async def remove_trading_pair(**kwargs):
        removed.append(dict(kwargs))
        connector._trading_pairs.clear()
        tracker._trading_pairs.clear()
        tracker.order_books.clear()
        return True

    market_data = types.SimpleNamespace(remove_trading_pair=remove_trading_pair)
    first = AccountTradingInterface(ConnectorService(), market_data, "master")
    second = AccountTradingInterface(ConnectorService(), market_data, "master")
    await first.add_market("generic", "BTC-USD")
    await second.add_market("generic", "BTC-USD")

    await first.remove_market("generic", "BTC-USD")

    CHECK.assertEqual([], removed)
    CHECK.assertNotIn("BTC-USD", first.markets.get("generic", set()))
    CHECK.assertIn("BTC-USD", second.markets["generic"])
    CHECK.assertIn("BTC-USD", tracker.order_books)

    residual = await first._finalize_failed_market_add(
        "generic", "BTC-USD", connector, connector_preexisting=True
    )
    CHECK.assertIsNone(residual)
    CHECK.assertEqual([], removed)
    CHECK.assertIn("BTC-USD", tracker.order_books)

    await second.remove_market("generic", "BTC-USD")
    CHECK.assertEqual(1, len(removed))
    CHECK.assertNotIn("BTC-USD", tracker.order_books)


@pytest.mark.asyncio
async def test_adjacent_b1_peer_only_pair_removal_rejects_without_mutation():
    from services.connector_capabilities import ConnectorCapabilityError
    from services.trading_service import AccountTradingInterface

    class ReadyBook:
        snapshot = ([1], [1])

    tracker = TransactionTracker()
    connector = TransactionConnector(tracker)
    connector._trading_pairs[:] = ["A-USD", "B-USD"]
    tracker._trading_pairs[:] = ["A-USD", "B-USD"]
    tracker.order_books.update({"A-USD": ReadyBook(), "B-USD": ReadyBook()})

    class ConnectorService:
        def get_account_connectors(self, account):
            return {"generic": connector}

    removed = []
    market_data = types.SimpleNamespace(
        remove_trading_pair=lambda **kwargs: removed.append(kwargs)
    )
    first = AccountTradingInterface(ConnectorService(), market_data, "master")
    peer = AccountTradingInterface(ConnectorService(), market_data, "master")
    await first.add_market("generic", "A-USD")
    await peer.add_market("generic", "B-USD")

    with pytest.raises(ConnectorCapabilityError) as captured:
        await first.remove_market("generic", "B-USD")

    CHECK.assertEqual("market_ownership_required", captured.value.detail["code"])
    CHECK.assertEqual([], removed)
    CHECK.assertEqual({"A-USD"}, first.markets["generic"])
    CHECK.assertEqual({"B-USD"}, peer.markets["generic"])
    CHECK.assertEqual({"A-USD", "B-USD"}, set(tracker.order_books))


@pytest.mark.asyncio
async def test_adjacent_b3_tracker_suppressor_is_cancelled_once_and_retained():
    module, registry_type = _load_unified_service_module()
    service = _adjacent_unified_service(module, registry_type)
    release = asyncio.Event()
    cancelled_once = asyncio.Event()

    async def resistant():
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled_once.set()
            await release.wait()

    task = asyncio.create_task(resistant())
    await asyncio.sleep(0)

    class Tracker:
        _trading_pairs = ["BTC-USD"]
        _tracking_tasks = {"BTC-USD": task}
        order_books = {}

        def stop(self):
            return None

    connector = types.SimpleNamespace(order_book_tracker=Tracker())
    owner_key = ("master", "generic", "network")
    with pytest.raises(RuntimeError):
        await service._stop_connector_network(connector, "master", "generic")
    await cancelled_once.wait()
    CHECK.assertFalse(task.done())
    CHECK.assertIn(task, service._connector_residual_tasks[owner_key])

    with pytest.raises(RuntimeError):
        await service._stop_connector_network(connector, "master", "generic")
    CHECK.assertFalse(task.done())
    CHECK.assertIn(task, service._connector_residual_tasks[owner_key])

    release.set()
    await task
    await service._stop_connector_network(connector, "master", "generic")
    CHECK.assertNotIn(owner_key, service._connector_residual_tasks)


@pytest.mark.asyncio
async def test_adjacent_b3_empty_restart_never_publishes_private_owner():
    module, registry_type = _load_unified_service_module()
    service = _adjacent_unified_service(module, registry_type)
    tracker = types.SimpleNamespace(_trading_pairs=[])
    connector = types.SimpleNamespace(order_book_tracker=tracker)
    service.get_best_connector_for_market = lambda name, account=None: connector

    result = await service.restart_order_book_tracker("generic", "master")

    CHECK.assertIs(False, result["success"])
    CHECK.assertEqual("no_trading_pairs_to_restart", result["error"])
    CHECK.assertNotIn(
        ("master", "generic", "restart"), service._connector_cleanup_owners
    )


@pytest.mark.asyncio
async def test_adjacent_b3_restart_retry_does_not_recancel_retained_task():
    module, registry_type = _load_unified_service_module()
    service = _adjacent_unified_service(module, registry_type)
    release = asyncio.Event()
    cancelled_once = asyncio.Event()

    async def resistant():
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled_once.set()
            await release.wait()

    task = asyncio.create_task(resistant())
    await asyncio.sleep(0)

    class Tracker:
        _trading_pairs = ["BTC-USD"]
        _tracking_tasks = {"BTC-USD": task}

        def stop(self):
            return None

        def start(self):
            return None

        async def wait_ready(self):
            return None

    tracker = Tracker()
    connector = types.SimpleNamespace(order_book_tracker=tracker)
    service.get_best_connector_for_market = lambda name, account=None: connector
    service._wait_for_websocket_ready = lambda *args, **kwargs: asyncio.sleep(
        0, result=True
    )
    owner_key = ("master", "generic", "restart")

    first = await service.restart_order_book_tracker("generic", "master")
    await cancelled_once.wait()
    CHECK.assertEqual("tracker_cleanup_pending", first["error"])
    CHECK.assertEqual(1, task.cancelling())
    CHECK.assertIn(task, service._connector_residual_tasks[owner_key])

    second = await service.restart_order_book_tracker("generic", "master")
    CHECK.assertEqual("tracker_cleanup_pending", second["error"])
    CHECK.assertEqual(1, task.cancelling())

    release.set()
    await task
    third = await service.restart_order_book_tracker("generic", "master")
    CHECK.assertIs(True, third["success"])
    CHECK.assertNotIn(owner_key, service._connector_cleanup_owners)


@pytest.mark.asyncio
async def test_adjacent_b4_explicit_account_never_uses_peer_for_market_operations():
    from services.market_data_service import MarketDataService

    module, registry_type = _load_unified_service_module()
    service = _adjacent_unified_service(module, registry_type)
    peer_calls = []

    class PeerConnector:
        order_book_tracker = types.SimpleNamespace(
            order_books={"BTC-USD": object()}, _trading_pairs=["BTC-USD"]
        )
        trading_rules = {}
        _orderbook_ds = types.SimpleNamespace()

        async def get_last_traded_prices(self, pairs):
            peer_calls.append("prices")
            return {pair: 1 for pair in pairs}

        async def remove_trading_pair(self, pair):
            peer_calls.append("remove")
            return True

    peer = PeerConnector()
    service._trading_connectors = {
        "account-a": {},
        "account-b": {"generic": peer},
    }
    service.get_data_connector = lambda name: None
    market_data = MarketDataService(service)

    CHECK.assertIsNone(service.get_best_connector_for_market("generic", "account-a"))
    CHECK.assertIs(peer, service.get_best_connector_for_market("generic"))
    CHECK.assertIsNone(market_data.get_order_book("generic", "BTC-USD", "account-a"))
    CHECK.assertIn(
        "error",
        await market_data.get_order_book_data(
            "generic", "BTC-USD", account_name="account-a"
        ),
    )
    CHECK.assertIn(
        "error",
        await market_data.get_order_book_query_result(
            "generic", "BTC-USD", True, account_name="account-a", volume=1
        ),
    )
    CHECK.assertIn(
        "error", await market_data.get_prices("generic", ["BTC-USD"], "account-a")
    )
    CHECK.assertIn(
        "error",
        await market_data.get_trading_rules(
            "generic", ["BTC-USD"], "account-a"
        ),
    )
    CHECK.assertIn(
        "error",
        await market_data.get_funding_info("generic", "BTC-USD", "account-a"),
    )
    CHECK.assertIs(
        False,
        await service.remove_trading_pair("generic", "BTC-USD", "account-a"),
    )
    restart = await service.restart_order_book_tracker("generic", "account-a")
    CHECK.assertIs(False, restart["success"])
    CHECK.assertEqual([], peer_calls)


@pytest.mark.asyncio
async def test_adjacent_b6_public_ticker_capability_is_typed_and_declared():
    from models import CapabilityErrorResponse
    from routers import market_data as market_data_router
    from services.connector_capabilities import (
        CapabilityErrorDetail,
        ConnectorCapabilityError,
        ConnectorFeature,
    )

    capability_error = ConnectorCapabilityError(
        CapabilityErrorDetail(
            code="unsupported_connector_feature",
            message="unsupported",
            connector="manzana",
            feature=ConnectorFeature.TICKERS.value,
        )
    )

    class Service:
        async def fetch_tickers_for(self, *args, **kwargs):
            return {}, {"manzana": capability_error}

        def collected_connector_names(self):
            return ["manzana"]

    with pytest.raises(ConnectorCapabilityError) as captured:
        await market_data_router.get_tickers(
            connectors=["manzana"],
            refresh=True,
            max_age=None,
            market_data_manager=Service(),
        )

    CHECK.assertEqual(400, captured.value.status_code)
    CHECK.assertEqual("unsupported_connector_feature", captured.value.detail["code"])
    ticker_route = next(
        route
        for route in market_data_router.router.routes
        if getattr(route, "path", None) == "/market-data/tickers"
    )
    CHECK.assertIs(CapabilityErrorResponse, ticker_route.responses[400]["model"])


@pytest.mark.asyncio
async def test_adjacent_b6_b8_mixed_ticker_errors_are_stable_and_sanitized():
    from routers import market_data as market_data_router

    marker = "api_key=REVIEWER-CREDENTIAL-MARKER"

    class Ticker:
        def to_dict(self):
            return {
                "price": 1.0,
                "base_volume": None,
                "quote_volume": None,
                "timestamp": 1.0,
            }

    class Service:
        async def fetch_tickers_for(self, *args, **kwargs):
            return {"good": {"BTC-USD": Ticker()}}, {"bad": RuntimeError(marker)}

        def collected_connector_names(self):
            return ["good", "bad"]

        def ticker_updated_at(self, name):
            return 1.0

    response = await market_data_router.get_tickers(
        connectors=["good", "bad"],
        refresh=True,
        max_age=None,
        market_data_manager=Service(),
    )
    serialized = response.model_dump_json()
    CHECK.assertNotIn(marker, serialized)
    CHECK.assertEqual("ticker_fetch_failed", response.errors["bad"].code)


@pytest.mark.asyncio
async def test_adjacent_b7_periodic_refresh_gates_before_connector_creation():
    from services.connector_capabilities import ConnectorCapabilityRegistry

    module = _load_accounts_service_module()
    initialized = []
    cycle_complete = asyncio.Event()

    class ConnectorService:
        registry = ConnectorCapabilityRegistry()

        def require_capability(self, connector, feature, trading_pair=None):
            self.registry.require(connector, feature, trading_pair)

        def list_available_credentials(self, account):
            return ["manzana"]

        def is_trading_connector_initialized(self, account, connector):
            return False

        async def get_trading_connector(self, account, connector):
            initialized.append((account, connector))
            return object()

        def get_all_trading_connectors(self):
            return {}

    async def gateway_refresh():
        cycle_complete.set()

    accounts = object.__new__(module.AccountsService)
    accounts._connector_service = ConnectorService()
    accounts.list_accounts = lambda: ["master"]
    accounts.accounts_state = {}
    accounts._update_gateway_balances = gateway_refresh
    accounts._mirror_gateway_state_to_accounts = lambda metadata: None
    accounts.dump_account_state = lambda: asyncio.sleep(0)
    accounts.update_account_state_interval = 0

    loop = asyncio.create_task(accounts.update_account_state_loop())
    await asyncio.wait_for(cycle_complete.wait(), timeout=1)
    loop.cancel()
    await asyncio.gather(loop, return_exceptions=True)
    CHECK.assertEqual([], initialized)


@pytest.mark.asyncio
async def test_adjacent_b7_duplicate_websocket_failure_preserves_old_subscription():
    from services.connector_capabilities import ConnectorCapabilityRegistry

    module = _load_websocket_manager_module()

    class Task:
        cancelled = False

        def done(self):
            return False

        def cancel(self):
            self.cancelled = True

    old_task = Task()
    sub_id = "order_book_generic_BTC-USD"
    old = module.Subscription(
        subscription_id=sub_id,
        sub_type="order_book",
        connector="generic",
        trading_pair="BTC-USD",
        update_interval=1.0,
        task=old_task,
    )

    class MarketData:
        registry = ConnectorCapabilityRegistry()

        def __init__(self):
            self.fail = True

        def require_capability(self, connector, feature, trading_pair=None):
            self.registry.require(connector, feature, trading_pair)

        async def initialize_order_book(self, connector, trading_pair):
            if self.fail:
                raise ValueError("invalid pair")
            return True

    class WebSocket:
        async def send_json(self, payload):
            return None

    market_data = MarketData()
    manager = module.WebSocketManager(market_data)
    manager._connections = {"connection": {sub_id: old}}
    await manager.handle_subscribe(
        "connection",
        WebSocket(),
        {
            "type": "order_book",
            "connector": "generic",
            "trading_pair": "BTC-USD",
        },
    )

    CHECK.assertIs(old, manager._connections["connection"][sub_id])
    CHECK.assertFalse(old_task.cancelled)

    market_data.fail = False
    await manager.handle_subscribe(
        "connection",
        WebSocket(),
        {
            "type": "order_book",
            "connector": "generic",
            "trading_pair": "BTC-USD",
        },
    )
    CHECK.assertIsNot(old, manager._connections["connection"][sub_id])
    CHECK.assertTrue(old_task.cancelled)
    manager.shutdown()


@pytest.mark.asyncio
async def test_adjacent_b8_ticker_constructor_and_symbol_map_errors_are_sanitized(monkeypatch):
    from services.connector_capabilities import ConnectorCapabilityRegistry
    from services.market_data_service import MarketDataService
    from services.ticker_sources import TickerUnsupportedError

    marker = "api_key=REVIEWER-CREDENTIAL-MARKER"

    class ConnectorService:
        registry = ConnectorCapabilityRegistry()

        def require_capability(self, connector, feature, trading_pair=None):
            self.registry.require(connector, feature, trading_pair)

        def is_known_connector(self, connector):
            return True

        def get_best_connector_for_market(self, connector):
            raise RuntimeError(marker)

    service = MarketDataService(ConnectorService())
    market_module = sys.modules[MarketDataService.__module__]
    monkeypatch.setattr(market_module, "is_market_data_connector", lambda name: True)
    with pytest.raises(TickerUnsupportedError) as constructor_error:
        await service.fetch_connector_tickers("generic", force=True)
    CHECK.assertNotIn(marker, str(constructor_error.value))

    async def failed_fetch(*args, **kwargs):
        raise RuntimeError(marker)

    service.fetch_connector_tickers = failed_fetch
    _, stored_errors = await service.fetch_tickers_for(["generic"], force=True)
    CHECK.assertNotIn(marker, str(stored_errors))

    module_name = "issue222_adjacent_ticker_sources_under_test"
    spec = importlib.util.spec_from_file_location(
        module_name,
        Path(__file__).resolve().parents[1] / "services" / "ticker_sources.py",
    )
    ticker_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(ticker_module)

    class Connector:
        async def trading_pair_symbol_map(self):
            raise RuntimeError(marker)

    with pytest.raises(ticker_module.TickerFetchError) as symbol_error:
        await ticker_module._load_symbol_map(Connector(), "generic")
    CHECK.assertNotIn(marker, str(symbol_error.value))


def test_adjacent_b8_candle_stop_failure_preserves_feed_and_lock():
    from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
    from services.market_data_service import FeedType, MarketDataService

    class Feed:
        def stop(self):
            raise RuntimeError("api_key=REVIEWER-CREDENTIAL-MARKER")

    service = MarketDataService(types.SimpleNamespace())
    config = CandlesConfig(
        connector="generic",
        trading_pair="BTC-USD",
        interval="1m",
        max_records=10,
    )
    key = service._generate_feed_key(
        FeedType.CANDLES, config.connector, config.trading_pair, config.interval
    )
    feed = Feed()
    lock = asyncio.Lock()
    service._candle_feeds[key] = feed
    service._candle_feed_locks[key] = lock

    service.stop_candle_feed(config)

    CHECK.assertIs(feed, service._candle_feeds[key])
    CHECK.assertIs(lock, service._candle_feed_locks[key])


@pytest.mark.asyncio
async def test_adjacent_b8_prices_and_funding_never_store_exception_values():
    from services.connector_capabilities import ConnectorCapabilityRegistry
    from services.market_data_service import MarketDataService

    marker = "api_key=REVIEWER-CREDENTIAL-MARKER"

    class DataSource:
        async def get_funding_info(self, pair):
            raise RuntimeError(marker)

    class Connector:
        _orderbook_ds = DataSource()

        async def get_last_traded_prices(self, pairs):
            raise RuntimeError(marker)

    class ConnectorService:
        registry = ConnectorCapabilityRegistry()

        def require_capability(self, connector, feature, trading_pair=None):
            self.registry.require(connector, feature, trading_pair)

        def get_best_connector_for_market(self, connector, account=None):
            return Connector()

    service = MarketDataService(ConnectorService())
    prices = await service.get_prices("generic", ["BTC-USD"], "master")
    funding = await service.get_funding_info("generic", "BTC-USD", "master")
    serialized = json.dumps({"prices": prices, "funding": funding})

    CHECK.assertNotIn(marker, serialized)
    CHECK.assertEqual("price_fetch_failed", prices["error"]["code"])
    CHECK.assertEqual("funding_fetch_failed", funding["error"]["code"])


@pytest.mark.asyncio
async def test_adjacent_b8_price_and_funding_routes_never_publish_exception_values():
    from fastapi import HTTPException
    from routers import market_data as market_data_router

    marker = "api_key=REVIEWER-CREDENTIAL-MARKER"

    class Service:
        def require_capability(self, *args, **kwargs):
            return None

        async def get_prices(self, *args, **kwargs):
            raise RuntimeError(marker)

        async def get_funding_info(self, *args, **kwargs):
            raise RuntimeError(marker)

    with pytest.raises(HTTPException) as price_error:
        await market_data_router.get_prices(
            market_data_router.PriceRequest(
                connector_name="generic", trading_pairs=["BTC-USD"]
            ),
            market_data_manager=Service(),
        )
    with pytest.raises(HTTPException) as funding_error:
        await market_data_router.get_funding_info(
            market_data_router.FundingInfoRequest(
                connector_name="generic_perpetual", trading_pair="BTC-USD"
            ),
            market_data_manager=Service(),
        )

    serialized = json.dumps(
        {"price": price_error.value.detail, "funding": funding_error.value.detail}
    )
    CHECK.assertNotIn(marker, serialized)
    CHECK.assertEqual("price_fetch_failed", price_error.value.detail["code"])
    CHECK.assertEqual("funding_fetch_failed", funding_error.value.detail["code"])


@pytest.mark.asyncio
async def test_final_adjacent_b2_index_stop_terminal_retry_is_consumed_once():
    module, registry_type = _load_unified_service_module()
    service = _adjacent_unified_service(module, registry_type)
    release = asyncio.Event()

    class Index:
        def __init__(self):
            self.stop_calls = 0

        async def stop(self):
            self.stop_calls += 1
            if self.stop_calls == 1:
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    await release.wait()

    index = Index()
    with pytest.raises(RuntimeError):
        await service._stop_owned_index("master", "generic", "orders", index)
    release.set()
    await asyncio.sleep(0)

    await service._stop_owned_index("master", "generic", "orders", index)

    CHECK.assertEqual(1, index.stop_calls)
    CHECK.assertNotIn(
        ("master", "generic", "orders"), service._connector_cleanup_owners
    )


@pytest.mark.asyncio
async def test_final_adjacent_b2_network_tracker_terminal_retry_is_consumed_once():
    module, registry_type = _load_unified_service_module()
    service = _adjacent_unified_service(module, registry_type)
    release = asyncio.Event()

    class Tracker:
        order_books = {}
        _tracking_tasks = {}

        def __init__(self):
            self.stop_calls = 0

        async def stop(self):
            self.stop_calls += 1
            if self.stop_calls == 1:
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    await release.wait()

    tracker = Tracker()
    connector = types.SimpleNamespace(order_book_tracker=tracker)
    with pytest.raises(RuntimeError):
        await service._stop_connector_network(connector, "master", "generic")
    release.set()
    await asyncio.sleep(0)

    await service._stop_connector_network(connector, "master", "generic")

    CHECK.assertEqual(1, tracker.stop_calls)
    CHECK.assertNotIn(
        ("master", "generic", "network"), service._connector_cleanup_owners
    )


@pytest.mark.asyncio
async def test_final_adjacent_b2_restart_stop_terminal_retry_is_consumed_once():
    module, registry_type = _load_unified_service_module()
    service = _adjacent_unified_service(module, registry_type)
    release = asyncio.Event()

    class Tracker:
        _trading_pairs = ["BTC-USD"]
        _tracking_tasks = {}
        order_books = {}

        def __init__(self):
            self.stop_calls = 0

        async def stop(self):
            self.stop_calls += 1
            if self.stop_calls == 1:
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    await release.wait()

        def start(self):
            return None

        async def wait_ready(self):
            return None

    tracker = Tracker()
    connector = types.SimpleNamespace(order_book_tracker=tracker)
    service.get_best_connector_for_market = lambda name, account=None: connector
    service._wait_for_websocket_ready = lambda *args, **kwargs: asyncio.sleep(
        0, result=True
    )

    first = await service.restart_order_book_tracker("generic", "master")
    CHECK.assertEqual("tracker_cleanup_pending", first["error"])
    release.set()
    await asyncio.sleep(0)
    second = await service.restart_order_book_tracker("generic", "master")

    CHECK.assertIs(True, second["success"])
    CHECK.assertEqual(1, tracker.stop_calls)


@pytest.mark.asyncio
async def test_final_adjacent_b2_cancelled_restart_retains_exact_stop_task():
    module, registry_type = _load_unified_service_module()
    service = _adjacent_unified_service(module, registry_type)
    entered = asyncio.Event()
    release = asyncio.Event()
    stop_tasks = []

    class Tracker:
        _trading_pairs = ["BTC-USD"]
        _tracking_tasks = {}
        order_books = {}

        def __init__(self):
            self.stop_calls = 0

        async def stop(self):
            self.stop_calls += 1
            stop_tasks.append(asyncio.current_task())
            entered.set()
            await release.wait()

        def start(self):
            return None

        async def wait_ready(self):
            return None

    tracker = Tracker()
    connector = types.SimpleNamespace(order_book_tracker=tracker)
    service.get_best_connector_for_market = lambda name, account=None: connector
    owner_key = ("master", "generic", "restart")
    caller = asyncio.create_task(
        service.restart_order_book_tracker("generic", "master")
    )
    await entered.wait()
    caller.cancel()
    with pytest.raises(asyncio.CancelledError):
        await caller

    retained_exact_task = stop_tasks[0] in service._connector_residual_tasks.get(
        owner_key, set()
    )
    retry = await service.restart_order_book_tracker("generic", "master")
    calls_before_release = tracker.stop_calls
    release.set()
    await asyncio.gather(*stop_tasks, return_exceptions=True)

    CHECK.assertEqual("tracker_cleanup_pending", retry["error"])
    CHECK.assertTrue(retained_exact_task)
    CHECK.assertEqual(1, calls_before_release)


@pytest.mark.asyncio
async def test_final_adjacent_b2_restart_requires_remote_readiness():
    module, registry_type = _load_unified_service_module()
    service = _adjacent_unified_service(module, registry_type)

    class Tracker:
        _trading_pairs = ["BTC-USD"]
        _tracking_tasks = {}
        order_books = {}

        def stop(self):
            return None

        def start(self):
            return None

        async def wait_ready(self):
            return None

    tracker = Tracker()
    connector = types.SimpleNamespace(order_book_tracker=tracker)
    service.get_best_connector_for_market = lambda name, account=None: connector
    service._wait_for_websocket_ready = lambda *args, **kwargs: asyncio.sleep(
        0, result=False
    )
    owner_key = ("master", "generic", "restart")

    result = await service.restart_order_book_tracker("generic", "master")

    CHECK.assertIs(False, result["success"])
    CHECK.assertEqual("tracker_websocket_not_ready", result["error"])
    CHECK.assertIn(owner_key, service._connector_cleanup_owners)


@pytest.mark.asyncio
async def test_final_adjacent_b2_stop_all_cleans_private_restart_owner():
    module, registry_type = _load_unified_service_module()
    service = _adjacent_unified_service(module, registry_type)

    class Tracker:
        _tracking_tasks = {}
        order_books = {}

        def __init__(self):
            self.stop_calls = 0

        def stop(self):
            self.stop_calls += 1
            return None

    tracker = Tracker()
    owner_key = ("master", "generic", "restart")
    service._connector_cleanup_owners[owner_key] = tracker

    await service.stop_all()

    CHECK.assertEqual(1, tracker.stop_calls)
    CHECK.assertNotIn(owner_key, service._connector_cleanup_owners)


@pytest.mark.asyncio
async def test_final_adjacent_b6_false_websocket_validation_preserves_old_subscription():
    from services.connector_capabilities import ConnectorCapabilityRegistry

    module = _load_websocket_manager_module()

    class Task:
        cancelled = False

        def done(self):
            return False

        def cancel(self):
            self.cancelled = True

    old_task = Task()
    sub_id = "order_book_generic_BTC-USD"
    old = module.Subscription(
        subscription_id=sub_id,
        sub_type="order_book",
        connector="generic",
        trading_pair="BTC-USD",
        update_interval=1.0,
        task=old_task,
    )

    class MarketData:
        registry = ConnectorCapabilityRegistry()

        def require_capability(self, connector, feature, trading_pair=None):
            self.registry.require(connector, feature, trading_pair)

        async def initialize_order_book(self, connector, trading_pair):
            return False

    class WebSocket:
        def __init__(self):
            self.payloads = []

        async def send_json(self, payload):
            self.payloads.append(payload)

    websocket = WebSocket()
    manager = module.WebSocketManager(MarketData())
    manager._connections = {"connection": {sub_id: old}}
    await manager.handle_subscribe(
        "connection",
        websocket,
        {
            "type": "order_book",
            "connector": "generic",
            "trading_pair": "BTC-USD",
        },
    )

    CHECK.assertIs(old, manager._connections["connection"][sub_id])
    CHECK.assertFalse(old_task.cancelled)
    CHECK.assertEqual("feed_validation_failed", websocket.payloads[-1]["message"])
    manager.shutdown()


def test_final_adjacent_b8_diagnostics_task_errors_are_sanitized():
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from routers import market_data as market_data_router
    from services.market_data_service import MarketDataService

    module, registry_type = _load_unified_service_module()
    service = _adjacent_unified_service(module, registry_type)
    marker = "api_key=INDEPENDENT-REVIEW-CREDENTIAL"

    class FailedTask:
        def done(self):
            return True

        def cancelled(self):
            return False

        def exception(self):
            return RuntimeError(marker)

    tracker = types.SimpleNamespace(
        ready=False,
        _trading_pairs=["BTC-USD"],
        order_books={},
        _order_book_stream_listener_task=FailedTask(),
    )
    connector = types.SimpleNamespace(order_book_tracker=tracker)
    service.get_best_connector_for_market = lambda name, account=None: connector
    market_data = MarketDataService(service)
    app = FastAPI()
    app.include_router(market_data_router.router)
    app.dependency_overrides[market_data_router.get_market_data_service] = (
        lambda: market_data
    )

    response = TestClient(app).get(
        "/market-data/order-book/diagnostics/generic"
    )
    body = response.json()
    serialized = json.dumps(body)

    CHECK.assertEqual(200, response.status_code, response.text)
    CHECK.assertNotIn(marker, serialized)
    CHECK.assertEqual(
        "task_failed",
        body["tasks"]["_order_book_stream_listener_task"]["error"]["code"],
    )


def test_final_adjacent_b8_trading_rules_failures_are_typed_and_declared():
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from routers import connectors as connectors_router

    marker = "api_key=INDEPENDENT-REVIEW-CREDENTIAL"

    class Service:
        def __init__(self):
            self.raise_failure = False

        async def get_trading_rules(self, connector_name, trading_pairs):
            if self.raise_failure:
                raise RuntimeError(marker)
            return {
                "error": {
                    "code": "trading_rules_fetch_failed",
                    "message": marker,
                }
            }

    service = Service()
    app = FastAPI()
    app.include_router(connectors_router.router)
    app.state.market_data_service = service
    client = TestClient(app)
    returned_failure = client.get("/connectors/generic/trading-rules")
    service.raise_failure = True
    raised_failure = client.get("/connectors/generic/trading-rules")
    responses = app.openapi()["paths"][
        "/connectors/{connector_name}/trading-rules"
    ]["get"]["responses"]

    for response in (returned_failure, raised_failure):
        CHECK.assertEqual(500, response.status_code, response.text)
        CHECK.assertEqual("trading_rules_fetch_failed", response.json()["detail"]["code"])
        CHECK.assertNotIn(marker, response.text)
    CHECK.assertEqual(
        "#/components/schemas/PublicErrorResponse",
        responses["500"]["content"]["application/json"]["schema"]["$ref"],
    )


def test_final_adjacent_b8_order_book_missing_paths_are_typed_and_declared():
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from routers import market_data as market_data_router
    from services.market_data_service import MarketDataService

    class ConnectorService:
        def require_capability(self, *args, **kwargs):
            return None

        def get_best_connector_for_market(self, connector, account=None):
            return None

    market_data = MarketDataService(ConnectorService())
    app = FastAPI()
    app.include_router(market_data_router.router)
    app.dependency_overrides[market_data_router.get_market_data_service] = (
        lambda: market_data
    )
    client = TestClient(app)
    base = {
        "connector_name": "generic",
        "trading_pair": "BTC-USD",
        "is_buy": True,
    }
    requests = (
        ("/market-data/order-book", {"connector_name": "generic", "trading_pair": "BTC-USD"}),
        ("/market-data/order-book/price-for-volume", {**base, "volume": 1}),
        ("/market-data/order-book/volume-for-price", {**base, "price": 1}),
        ("/market-data/order-book/price-for-quote-volume", {**base, "quote_volume": 1}),
        ("/market-data/order-book/quote-volume-for-price", {**base, "price": 1}),
        ("/market-data/order-book/vwap-for-volume", {**base, "volume": 1}),
    )
    responses = [client.post(path, json=body) for path, body in requests]

    for response in responses:
        CHECK.assertEqual(500, response.status_code, response.text)
        CHECK.assertEqual("connector_not_found", response.json()["detail"]["code"])

    paths = app.openapi()["paths"]
    for path, _ in requests:
        CHECK.assertEqual(
            "#/components/schemas/PublicErrorResponse",
            paths[path]["post"]["responses"]["500"]["content"]["application/json"]["schema"]["$ref"],
        )

    connector = types.SimpleNamespace(
        order_book_tracker=types.SimpleNamespace(order_books={}),
        _orderbook_ds=None,
    )
    market_data._connector_service.get_best_connector_for_market = (
        lambda name, account=None: connector
    )
    missing_book = asyncio.run(
        market_data.get_order_book_query_result(
            "generic", "BTC-USD", True, volume=1
        )
    )
    CHECK.assertEqual("order_book_not_found", missing_book["error"]["code"])


def test_final_adjacent_b8_active_feed_failures_are_non_2xx_typed_and_declared():
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from routers import market_data as market_data_router

    marker = "api_key=INDEPENDENT-REVIEW-CREDENTIAL"

    class Service:
        def get_active_feeds_info(self):
            raise RuntimeError(marker)

    app = FastAPI()
    app.include_router(market_data_router.router)
    app.state.market_data_service = Service()
    response = TestClient(app).get("/market-data/active-feeds")
    responses = app.openapi()["paths"]["/market-data/active-feeds"]["get"][
        "responses"
    ]

    CHECK.assertEqual(500, response.status_code, response.text)
    CHECK.assertEqual("active_feeds_unavailable", response.json()["detail"]["code"])
    CHECK.assertNotIn(marker, response.text)
    CHECK.assertEqual(
        "#/components/schemas/PublicErrorResponse",
        responses["500"]["content"]["application/json"]["schema"]["$ref"],
    )


@pytest.mark.asyncio
async def test_lifecycle_b1_unscoped_restart_uses_physical_owner_lock_and_rejects_ambiguity():
    module, registry_type = _load_unified_service_module()
    service = _adjacent_unified_service(module, registry_type)
    first_stop_entered = asyncio.Event()
    overlapping_stop_entered = asyncio.Event()
    release_first_stop = asyncio.Event()

    class Tracker:
        _trading_pairs = ["BTC-USD"]
        _tracking_tasks = {}
        order_books = {}

        def __init__(self):
            self.stop_calls = 0
            self.active_stops = 0
            self.max_active_stops = 0

        async def stop(self):
            self.stop_calls += 1
            self.active_stops += 1
            self.max_active_stops = max(
                self.max_active_stops, self.active_stops
            )
            if self.active_stops > 1:
                overlapping_stop_entered.set()
            try:
                if self.stop_calls == 1:
                    first_stop_entered.set()
                    await release_first_stop.wait()
                else:
                    await asyncio.sleep(0)
            finally:
                self.active_stops -= 1

        def start(self):
            return None

        async def wait_ready(self):
            return True

    tracker = Tracker()
    connector = types.SimpleNamespace(order_book_tracker=tracker)
    service._trading_connectors = {"master": {"generic": connector}}
    service._wait_for_websocket_ready = lambda *args, **kwargs: asyncio.sleep(
        0, result=True
    )

    restart = asyncio.create_task(service.restart_order_book_tracker("generic"))
    await first_stop_entered.wait()
    stopping = asyncio.create_task(
        service.stop_trading_connector("master", "generic")
    )
    await asyncio.sleep(0.01)
    release_first_stop.set()
    results = await asyncio.gather(restart, stopping, return_exceptions=True)

    CHECK.assertFalse(
        any(isinstance(result, BaseException) for result in results), results
    )
    CHECK.assertEqual(1, tracker.max_active_stops)
    CHECK.assertEqual(2, tracker.stop_calls)

    ambiguous = _adjacent_unified_service(module, registry_type)

    class QuietTracker:
        _trading_pairs = ["BTC-USD"]
        _tracking_tasks = {}
        order_books = {}

        def __init__(self):
            self.stop_calls = 0

        def stop(self):
            self.stop_calls += 1

    first = QuietTracker()
    second = QuietTracker()
    ambiguous._trading_connectors = {
        "account-a": {
            "generic": types.SimpleNamespace(order_book_tracker=first)
        },
        "account-b": {
            "generic": types.SimpleNamespace(order_book_tracker=second)
        },
    }
    result = await ambiguous.restart_order_book_tracker("generic")
    CHECK.assertIs(False, result["success"])
    CHECK.assertEqual("ambiguous_connector_owner", result["error"])
    CHECK.assertEqual((0, 0), (first.stop_calls, second.stop_calls))


@pytest.mark.asyncio
async def test_lifecycle_b2_published_restart_owner_coalesces_final_stop():
    module, registry_type = _load_unified_service_module()
    service = _adjacent_unified_service(module, registry_type)

    class Tracker:
        _trading_pairs = ["BTC-USD"]
        _tracking_tasks = {}
        order_books = {}

        def __init__(self):
            self.stop_calls = 0

        def stop(self):
            self.stop_calls += 1

        def start(self):
            return None

        async def wait_ready(self):
            return True

    tracker = Tracker()
    connector = types.SimpleNamespace(order_book_tracker=tracker)
    service._trading_connectors = {"master": {"generic": connector}}
    service._wait_for_websocket_ready = lambda *args, **kwargs: asyncio.sleep(
        0, result=False
    )

    restart = await service.restart_order_book_tracker("generic", "master")
    CHECK.assertEqual("tracker_websocket_not_ready", restart["error"])
    await service.stop_all()

    CHECK.assertEqual(2, tracker.stop_calls)
    CHECK.assertEqual({}, service._connector_cleanup_owners)
    CHECK.assertEqual({}, service._connector_cleanup_phases)
    CHECK.assertEqual({}, service._connector_residual_tasks)


@pytest.mark.asyncio
async def test_lifecycle_b3_cancelled_async_start_is_stopped_after_activation():
    module, registry_type = _load_unified_service_module()
    service = _adjacent_unified_service(module, registry_type)
    start_entered = asyncio.Event()
    release_start = asyncio.Event()

    class Tracker:
        _trading_pairs = ["BTC-USD"]
        _tracking_tasks = {}
        order_books = {}

        def __init__(self):
            self.active = True
            self.stop_calls = 0

        def stop(self):
            self.stop_calls += 1
            self.active = False

        async def start(self):
            start_entered.set()
            await release_start.wait()
            self.active = True

        async def wait_ready(self):
            return True

    tracker = Tracker()
    connector = types.SimpleNamespace(order_book_tracker=tracker)
    service._trading_connectors = {"master": {"generic": connector}}
    service._wait_for_websocket_ready = lambda *args, **kwargs: asyncio.sleep(
        0, result=True
    )
    owner_key = ("master", "generic", "restart")

    caller = asyncio.create_task(
        service.restart_order_book_tracker("generic", "master")
    )
    await start_entered.wait()
    caller.cancel()
    with pytest.raises(asyncio.CancelledError):
        await caller
    start_task = service._connector_cleanup_phases[owner_key]["restart_start"]["task"]
    CHECK.assertIn(start_task, service._connector_residual_tasks[owner_key])

    shutdown = asyncio.create_task(service.stop_all())
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    release_start.set()
    await shutdown

    CHECK.assertFalse(tracker.active)
    CHECK.assertEqual(2, tracker.stop_calls)
    CHECK.assertEqual({}, service._connector_cleanup_owners)
    CHECK.assertEqual({}, service._connector_cleanup_phases)
    CHECK.assertEqual({}, service._connector_residual_tasks)


@pytest.mark.asyncio
async def test_lifecycle_b4_websocket_replacement_validation_is_read_only():
    from services.connector_capabilities import ConnectorCapabilityRegistry

    module = _load_websocket_manager_module()

    class Task:
        def __init__(self):
            self.cancelled = False

        def done(self):
            return False

        def cancel(self):
            self.cancelled = True

    class WebSocket:
        def __init__(self):
            self.payloads = []

        async def send_json(self, payload):
            self.payloads.append(payload)

    class Tracker:
        def __init__(self):
            self._trading_pairs = ["BTC-USD"]
            self.order_books = {"BTC-USD": object()}

    tracker = Tracker()
    connector = types.SimpleNamespace(
        _trading_pairs=["BTC-USD"], order_book_tracker=tracker
    )
    initialization_calls = []

    class ReadOnlyValidationService:
        registry = ConnectorCapabilityRegistry()

        def require_capability(self, connector_name, feature, trading_pair=None):
            self.registry.require(connector_name, feature, trading_pair)

        def validate_order_book_ready(self, connector_name, trading_pair):
            return False

        async def initialize_order_book(self, connector_name, trading_pair):
            initialization_calls.append((connector_name, trading_pair))
            connector._trading_pairs.clear()
            tracker._trading_pairs.clear()
            tracker.order_books.clear()
            return False

    old_task = Task()
    sub_id = "order_book_generic_BTC-USD"
    old = module.Subscription(
        subscription_id=sub_id,
        sub_type="order_book",
        connector="generic",
        trading_pair="BTC-USD",
        update_interval=1.0,
        task=old_task,
    )
    websocket = WebSocket()
    manager = module.WebSocketManager(ReadOnlyValidationService())
    manager._connections = {"connection": {sub_id: old}}
    await manager.handle_subscribe(
        "connection",
        websocket,
        {
            "type": "order_book",
            "connector": "generic",
            "trading_pair": "BTC-USD",
        },
    )

    CHECK.assertIs(old, manager._connections["connection"][sub_id])
    CHECK.assertFalse(old_task.cancelled)
    CHECK.assertEqual([], initialization_calls)
    CHECK.assertEqual(["BTC-USD"], connector._trading_pairs)
    CHECK.assertEqual(["BTC-USD"], tracker._trading_pairs)
    CHECK.assertEqual(["BTC-USD"], list(tracker.order_books))
    CHECK.assertEqual("feed_validation_failed", websocket.payloads[-1]["message"])

    fallback_calls = []

    class CompatibilityService:
        registry = ConnectorCapabilityRegistry()

        def __init__(self):
            self._connector_service = types.SimpleNamespace(
                _trading_connectors={"master": {"generic": connector}},
                _data_connectors={},
            )

        def require_capability(self, connector_name, feature, trading_pair=None):
            self.registry.require(connector_name, feature, trading_pair)

        async def initialize_order_book(self, connector_name, trading_pair):
            fallback_calls.append((connector_name, trading_pair))
            return False

    fallback_old_task = Task()
    fallback_old = module.Subscription(
        subscription_id=sub_id,
        sub_type="order_book",
        connector="generic",
        trading_pair="BTC-USD",
        update_interval=1.0,
        task=fallback_old_task,
    )
    fallback = module.WebSocketManager(CompatibilityService())
    fallback._connections = {"connection": {sub_id: fallback_old}}
    await fallback.handle_subscribe(
        "connection",
        WebSocket(),
        {
            "type": "order_book",
            "connector": "generic",
            "trading_pair": "BTC-USD",
        },
    )

    CHECK.assertEqual([], fallback_calls)
    CHECK.assertTrue(fallback_old_task.cancelled)
    CHECK.assertEqual(["BTC-USD"], connector._trading_pairs)
    CHECK.assertEqual(["BTC-USD"], tracker._trading_pairs)
    CHECK.assertEqual(["BTC-USD"], list(tracker.order_books))
    fallback.shutdown()


@pytest.mark.asyncio
async def test_lifecycle_b5_terminal_remote_cleanup_result_is_consumed_once():
    from services.connector_capabilities import ConnectorCapabilityError
    from services.trading_service import AccountTradingInterface

    class ReadyBook:
        snapshot = ([1], [1])

    tracker = TransactionTracker()
    connector = TransactionConnector(tracker)
    connector_service = TransactionConnectorService(connector)
    cleanup_entered = asyncio.Event()
    cleanup_release = asyncio.Event()
    initialize_calls = 0
    remove_calls = 0

    async def initialize_order_book(**kwargs):
        nonlocal initialize_calls
        initialize_calls += 1
        connector._trading_pairs.append("BTC-USD")
        tracker._trading_pairs.append("BTC-USD")
        tracker.order_books["BTC-USD"] = ReadyBook()
        return initialize_calls > 1

    async def remove_trading_pair(**kwargs):
        nonlocal remove_calls
        remove_calls += 1
        if remove_calls == 1:
            cleanup_entered.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await cleanup_release.wait()
        return True

    interface = AccountTradingInterface(
        connector_service,
        types.SimpleNamespace(
            initialize_order_book=initialize_order_book,
            remove_trading_pair=remove_trading_pair,
        ),
        "master",
    )
    with pytest.raises(ConnectorCapabilityError):
        await interface.add_market("generic", "BTC-USD")
    await cleanup_entered.wait()
    CHECK.assertEqual(1, remove_calls)

    cleanup_release.set()
    retained = {
        task
        for tasks in interface._residual_tasks.values()
        for task in tasks
    }
    await asyncio.gather(*retained)
    await interface.add_market("generic", "BTC-USD")

    CHECK.assertEqual(1, remove_calls)
    CHECK.assertEqual({}, interface.cleanup_residuals)
    CHECK.assertEqual({}, interface._residual_tasks)


@pytest.mark.asyncio
async def test_lifecycle_b6_interface_cleanup_releases_tokens_and_records_sole_owner():
    from services.trading_service import AccountTradingInterface

    class ReadyBook:
        snapshot = ([1], [1])

    tracker = TransactionTracker("BTC-USD", ReadyBook())
    connector = TransactionConnector(tracker, "BTC-USD")

    class ConnectorService:
        def get_account_connectors(self, account):
            return {"generic": connector}

    remove_calls = []

    async def remove_trading_pair(**kwargs):
        remove_calls.append(dict(kwargs))
        connector._trading_pairs.clear()
        tracker._trading_pairs.clear()
        tracker.order_books.clear()
        return True

    connector_service = ConnectorService()
    market_data = types.SimpleNamespace(remove_trading_pair=remove_trading_pair)
    first = AccountTradingInterface(connector_service, market_data, "master")
    second = AccountTradingInterface(connector_service, market_data, "master")
    await first.add_market("generic", "BTC-USD")
    await second.add_market("generic", "BTC-USD")

    await first.cleanup()
    CHECK.assertEqual({}, first.markets)
    CHECK.assertFalse(first._owns_market_pair("generic", "BTC-USD"))
    CHECK.assertEqual(1, len(second._market_owner_set("generic", "BTC-USD")))
    await second.remove_market("generic", "BTC-USD")
    CHECK.assertEqual(1, len(remove_calls))
    CHECK.assertEqual(set(), second._market_owner_set("generic", "BTC-USD"))

    connector._trading_pairs[:] = ["ETH-USD"]
    tracker._trading_pairs[:] = ["ETH-USD"]
    tracker.order_books["ETH-USD"] = ReadyBook()
    sole = AccountTradingInterface(connector_service, market_data, "master")
    await sole.add_market("generic", "ETH-USD")
    await sole.cleanup()

    residual_key = ("master", "generic", "pair", "ETH-USD")
    CHECK.assertFalse(sole._owns_market_pair("generic", "ETH-USD"))
    CHECK.assertIn(("generic", "ETH-USD"), sole.cleanup_residuals)
    CHECK.assertIn(residual_key, sole._cleanup_owners)
    CHECK.assertIs(connector, sole._cleanup_owners[residual_key]["connector"])
    CHECK.assertEqual(1, len(remove_calls))
