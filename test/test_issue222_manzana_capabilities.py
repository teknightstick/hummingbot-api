"""API-local command semantics for Issue #222.

The production image supplies Hummingbot's compiled package.  The isolated Windows
contract runner used for this issue does not, so a per-test import fixture exposes
only the names needed to import the API service and restores ``sys.modules`` after
each test. Behavioral fakes implement the same pinned OrderBook primitive contracts.
"""

import asyncio
import ast
import copy
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
from pydantic import BaseModel, ValidationError


CHECK = unittest.TestCase()


def _module(name: str) -> types.ModuleType:
    module = sys.modules.get(name)
    if module is None:
        module = types.ModuleType(name)
        module.__issue222_contract_shim__ = True
        sys.modules[name] = module
    return module


def _install_import_contract(*, force: bool = False) -> None:
    real_core_available = importlib.util.find_spec("hummingbot") is not None
    if real_core_available and not force:
        return

    if not real_core_available:
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
    models.GatewayWalletCredential = type(
        "GatewayWalletCredential",
        (BaseModel,),
        {
            "__annotations__": {
                "chain": str,
                "private_key": str,
                "set_default": bool,
            },
            "set_default": True,
        },
    )
    models.SetDefaultWalletRequest = type(
        "SetDefaultWalletRequest",
        (BaseModel,),
        {"__annotations__": {"chain": str, "address": str}},
    )
    routers = _module("routers")
    routers.__path__ = [str(Path(__file__).resolve().parents[1] / "routers")]

    if not real_core_available:
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
    deps.require_gateway_online = lambda: None

    accounts_service = _module("services.accounts_service")
    accounts_service.AccountsService = type("AccountsService", (), {})

    def validate_safe_name(name, label="name"):
        import re

        if not name or re.fullmatch(r"[A-Za-z0-9_-]+", name) is None:
            from models.connectors import CapabilityErrorDetail
            from services.connector_capabilities import ConnectorCapabilityError

            raise ConnectorCapabilityError(
                CapabilityErrorDetail(
                    code="invalid_account_resource_name",
                    message=f"Invalid {label}",
                    connector=name or "account",
                    feature="account_mutation",
                    reason="invalid_name",
                )
            )
        return name

    accounts_service.validate_safe_name = validate_safe_name
    history_service = _module("services.trading_history_service")
    history_service.TradingHistoryService = type("TradingHistoryService", (), {})

    config = _module("config")
    config.settings = types.SimpleNamespace(
        market_data=types.SimpleNamespace(candles_ready_timeout=1.0)
    )

    if not real_core_available:
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
    return name in {"deps", "config", "database"} or name.startswith(
        ("database.", "hummingbot", "models", "routers", "services", "utils")
    )


@pytest.fixture(autouse=True)
def _scoped_import_contract(request):
    """Install dependency-light shims only while an individual test executes."""

    if request.node.name in {
        "test_b6_capability_json_is_deterministic_across_hash_seeds",
        "test_b7_contract_module_does_not_pollute_imports_or_use_bare_asserts",
        "test_b9_fixture_database_shim_does_not_poison_real_import_order",
        "test_final_b8_fixture_restores_preexisting_module_dicts",
    }:
        yield
        return

    before = {
        name: (module, dict(module.__dict__))
        for name, module in sys.modules.items()
        if _is_contract_module(name)
    }
    _install_import_contract(force=request.node.name == "manual-fixture-check")
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
async def test_b9_quote_volume_result_is_fulfilled_quote_volume():
    """Mirror accepted core: result_volume is fulfilled quote, not base, volume."""

    class RealCoreShapedOrderBook:
        def get_price_for_quote_volume(self, is_buy, quote_volume):
            CHECK.assertIs(is_buy, True)
            CHECK.assertEqual(101.0, quote_volume)
            return QueryResult(float("nan"), quote_volume, 101.0, 101.0)

    service = MarketDataService(
        OrderBookConnectorService(RealCoreShapedOrderBook())
    )

    result = await service.get_order_book_query_result(
        "static", "BTC-USD", True, quote_volume=101.0
    )

    CHECK.assertEqual(101.0, result["result_price"])
    CHECK.assertEqual(101.0, result["result_quote_volume"])
    CHECK.assertNotIn("result_volume", result)


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


def _shift_successor_capabilities(*, supported_pairs=None, dynamic_subscription=True):
    from services.connector_capabilities import ConnectorFeature

    return {
        "supported_pairs": supported_pairs,
        "features": {
            ConnectorFeature.ORDER_BOOK,
            ConnectorFeature.ORDER_BOOK_QUERY,
            ConnectorFeature.LIVE_ORDER_BOOK,
            ConnectorFeature.TRADING_RULES,
            ConnectorFeature.BEST_BID_ASK,
            ConnectorFeature.DYNAMIC_SUBSCRIPTION,
        },
        "dynamic_subscription": dynamic_subscription,
        "perpetual": False,
        "candles": False,
        "last_trade": False,
    }


def test_connector_capability_registry_has_immutable_shift_bootstrap_truth():
    from models.connectors import ConnectorCapabilities
    from services.connector_capabilities import ConnectorCapabilityRegistry, ConnectorFeature

    registry = ConnectorCapabilityRegistry()
    manzana = registry.get("manzana")

    CHECK.assertIs(manzana.declared, True)
    CHECK.assertEqual("api-bootstrap", manzana.source)
    CHECK.assertIsNone(manzana.supported_pairs)
    CHECK.assertEqual(
        frozenset(_shift_successor_capabilities()["features"]),
        manzana.features,
    )
    CHECK.assertIs(manzana.dynamic_subscription, True)
    CHECK.assertIs(manzana.perpetual, False)
    CHECK.assertIs(manzana.candles, False)
    CHECK.assertIs(manzana.last_trade, False)
    CHECK.assertIn(ConnectorFeature.ORDER_BOOK, manzana.features)
    CHECK.assertIs(registry.supports_pair("manzana", "BTC-USD"), True)
    CHECK.assertIs(registry.supports_pair("manzana", "ETH-USD"), True)
    for unsupported in (
        ConnectorFeature.TICKERS,
        ConnectorFeature.RATES,
        ConnectorFeature.CANDLES,
        ConnectorFeature.PERPETUAL,
        ConnectorFeature.LAST_TRADE,
        ConnectorFeature.ORDER_SUBMISSION,
    ):
        CHECK.assertIs(registry.supports_feature("manzana", unsupported), False)

    with pytest.raises(ValidationError):
        manzana.dynamic_subscription = False

    injected = {
        "custom-bootstrap": ConnectorCapabilities(
            declared=True,
            source="unit-bootstrap",
            supported_pairs=frozenset({"ONE-USD"}),
            features=frozenset({ConnectorFeature.ORDER_BOOK}),
        )
    }
    injected_registry = ConnectorCapabilityRegistry(injected)
    injected.clear()
    CHECK.assertIs(
        injected_registry.supports_feature(
            "custom-bootstrap", ConnectorFeature.ORDER_BOOK
        ),
        True,
    )
    CHECK.assertIs(
        injected_registry.supports_pair("custom-bootstrap", "TWO-USD"), False
    )

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


def test_loaded_manzana_capabilities_narrow_pairs_and_mismatch_fails_closed():
    from services.connector_capabilities import (
        ConnectorCapabilityError,
        ConnectorCapabilityRegistry,
        ConnectorFeature,
    )

    registry = ConnectorCapabilityRegistry()
    discovered = types.SimpleNamespace(
        api_capabilities=_shift_successor_capabilities(
            supported_pairs={"BTC-USD", "ETH-USD"}
        )
    )
    loaded = registry.get("manzana", discovered)
    CHECK.assertEqual(frozenset({"BTC-USD", "ETH-USD"}), loaded.supported_pairs)
    CHECK.assertIs(registry.supports_pair("manzana", "ETH-USD", discovered), True)
    CHECK.assertIs(registry.supports_pair("manzana", "SOL-USD", discovered), False)

    truthful_addition = _shift_successor_capabilities(
        supported_pairs={"BTC-USD"}
    )
    truthful_addition["features"] = {
        *truthful_addition["features"],
        ConnectorFeature.ORDER_SUBMISSION,
    }
    loaded_with_addition = registry.get(
        "manzana",
        types.SimpleNamespace(
            api_capabilities=truthful_addition,
            buy=lambda **kwargs: "buy-id",
            sell=lambda **kwargs: "sell-id",
            cancel=lambda **kwargs: "cancel-id",
        ),
    )
    CHECK.assertEqual(frozenset({"BTC-USD"}), loaded_with_addition.supported_pairs)
    CHECK.assertIn(
        ConnectorFeature.ORDER_SUBMISSION.value,
        loaded_with_addition.features,
    )

    for required_feature in _shift_successor_capabilities()["features"]:
        missing_required = _shift_successor_capabilities(
            supported_pairs={"BTC-USD"}
        )
        missing_required["features"].remove(required_feature)
        if required_feature == ConnectorFeature.DYNAMIC_SUBSCRIPTION:
            missing_required["dynamic_subscription"] = False
        with pytest.raises(ConnectorCapabilityError) as missing:
            registry.get(
                "manzana",
                types.SimpleNamespace(api_capabilities=missing_required),
            )
        CHECK.assertEqual(
            "connector_capability_mismatch", missing.value.detail["code"]
        )

    mismatched = types.SimpleNamespace(
        api_capabilities=_shift_successor_capabilities(dynamic_subscription=False)
    )
    with pytest.raises(ConnectorCapabilityError) as mismatch:
        registry.require(
            "manzana",
            ConnectorFeature.LIVE_ORDER_BOOK,
            "BTC-USD",
            mismatched,
        )
    CHECK.assertEqual(400, mismatch.value.status_code)
    CHECK.assertEqual(
        {
            "code": "connector_capability_mismatch",
            "message": (
                "Connector 'manzana' capability declaration does not match "
                "its bootstrap declaration"
            ),
            "connector": "manzana",
            "feature": "capabilities",
            "reason": "bootstrap_instance_incompatible",
        },
        mismatch.value.detail,
    )

    with pytest.raises(ConnectorCapabilityError) as absent:
        registry.get("manzana", types.SimpleNamespace())
    CHECK.assertEqual(mismatch.value.detail, absent.value.detail)


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
    CHECK.assertEqual("api-bootstrap", capabilities.source)
    CHECK.assertIsNone(capabilities.supported_pairs)


def test_capabilities_route_typed_400_body_matches_openapi():
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from models.connectors import CapabilityErrorResponse
    from routers import connectors as connectors_router
    from services.connector_capabilities import ConnectorCapabilityRegistry

    class InvalidCapabilityConnectorService:
        def __init__(self):
            self.registry = ConnectorCapabilityRegistry()
            declaration = _shift_successor_capabilities(
                supported_pairs={"BTC-USD"}
            )
            declaration["features"] = set(declaration["features"]) - {
                "order_book"
            }
            self.connector = types.SimpleNamespace(api_capabilities=declaration)

        def get_connector_capabilities(self, connector_name):
            return self.registry.get(connector_name, self.connector)

        def is_known_connector(self, connector_name):
            return True

    app = FastAPI()
    app.include_router(connectors_router.router)
    app.state.market_data_service = types.SimpleNamespace(
        connector_service=InvalidCapabilityConnectorService()
    )
    response = TestClient(app).get("/connectors/manzana/capabilities")

    CHECK.assertEqual(400, response.status_code, response.text)
    CapabilityErrorResponse.model_validate(response.json())
    schema = app.openapi()["paths"]["/connectors/{connector_name}/capabilities"]
    CHECK.assertEqual(
        "#/components/schemas/CapabilityErrorResponse",
        schema["get"]["responses"]["400"]["content"]["application/json"][
            "schema"
        ]["$ref"],
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("trading_pair", ["BTC-USD", "ETH-USD"])
async def test_first_shift_book_request_passes_gates_and_constructs_connector(
    trading_pair,
):
    from services.connector_capabilities import ConnectorCapabilityRegistry

    module, _ = _load_unified_service_module()
    service = object.__new__(module.UnifiedConnectorService)
    service._capability_registry = ConnectorCapabilityRegistry()
    service._data_connectors = {}
    service._data_connectors_started = {}
    service._trading_connectors = {}
    construction_calls = []

    class ReadyBook:
        snapshot = ([object()], [object()])

    def construct(connector_name):
        construction_calls.append(connector_name)
        return types.SimpleNamespace(
            api_capabilities=_shift_successor_capabilities(
                supported_pairs={"BTC-USD", "ETH-USD"}
            ),
            _trading_pairs=[trading_pair],
            order_book_tracker=types.SimpleNamespace(
                _trading_pairs=[trading_pair],
                order_books={trading_pair: ReadyBook()},
            ),
        )

    service._create_data_connector = construct

    ready = await service.initialize_order_book("manzana", trading_pair)

    CHECK.assertIs(ready, True)
    CHECK.assertEqual(["manzana"], construction_calls)
    CHECK.assertIn("manzana", service._data_connectors)


@pytest.mark.asyncio
async def test_unsupported_feature_rejects_before_connector_lookup():
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

    with pytest.raises(ConnectorCapabilityError) as feature_error:
        await service.get_prices("manzana", ["ETH-USD"])
    CHECK.assertEqual(400, feature_error.value.status_code)
    CHECK.assertEqual("unsupported_connector_feature", feature_error.value.detail["code"])
    CHECK.assertEqual(ConnectorFeature.LAST_TRADE.value, feature_error.value.detail["feature"])
    CapabilityErrorResponse.model_validate({"detail": feature_error.value.detail})
    CHECK.assertEqual(0, connector_service.connector_lookups)


def test_mismatched_constructed_connector_is_not_published():
    from services.connector_capabilities import (
        ConnectorCapabilityError,
        ConnectorCapabilityRegistry,
    )

    module, _ = _load_unified_service_module()
    service = object.__new__(module.UnifiedConnectorService)
    service._capability_registry = ConnectorCapabilityRegistry()
    service._data_connectors = {}
    service._trading_connectors = {}
    constructed = types.SimpleNamespace(
        api_capabilities=_shift_successor_capabilities(
            dynamic_subscription=False
        )
    )
    service._create_data_connector = lambda connector_name: constructed

    with pytest.raises(ConnectorCapabilityError) as mismatch:
        service.get_data_connector("manzana")

    CHECK.assertEqual(
        "connector_capability_mismatch", mismatch.value.detail["code"]
    )
    CHECK.assertEqual({}, service._data_connectors)


def test_instance_only_order_submission_requires_complete_chain_before_publication():
    from services.connector_capabilities import (
        ConnectorCapabilityError,
        ConnectorCapabilityRegistry,
        ConnectorFeature,
    )

    module, _ = _load_unified_service_module()

    def loaded_connector(**members):
        declaration = _shift_successor_capabilities(
            supported_pairs={"BTC-USD"}
        )
        declaration["features"] = {
            *declaration["features"],
            ConnectorFeature.ORDER_SUBMISSION,
        }
        return types.SimpleNamespace(api_capabilities=declaration, **members)

    complete = loaded_connector(
        buy=lambda **kwargs: "buy-id",
        sell=lambda **kwargs: "sell-id",
        cancel=lambda **kwargs: "cancel-id",
    )
    accepted_service = object.__new__(module.UnifiedConnectorService)
    accepted_service._capability_registry = ConnectorCapabilityRegistry()
    accepted_service._data_connectors = {}
    accepted_service._trading_connectors = {}
    accepted_service._create_data_connector = lambda connector_name: complete

    CHECK.assertIs(
        complete, accepted_service.get_data_connector("manzana")
    )
    CHECK.assertIs(complete, accepted_service._data_connectors["manzana"])

    incomplete = loaded_connector(
        buy=lambda **kwargs: "buy-id",
        sell=lambda **kwargs: "sell-id",
    )
    rejected_service = object.__new__(module.UnifiedConnectorService)
    rejected_service._capability_registry = ConnectorCapabilityRegistry()
    rejected_service._data_connectors = {}
    rejected_service._trading_connectors = {}
    rejected_service._create_data_connector = lambda connector_name: incomplete

    with pytest.raises(ConnectorCapabilityError) as mismatch:
        rejected_service.get_data_connector("manzana")

    CHECK.assertEqual(
        "connector_capability_mismatch", mismatch.value.detail["code"]
    )
    CHECK.assertEqual({}, rejected_service._data_connectors)


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
async def test_shift_connector_add_reaches_dynamic_initialization():
    from models.market_data import AddTradingPairRequest
    from routers.market_data import add_trading_pair
    from services.connector_capabilities import ConnectorCapabilityRegistry

    class GuardedConnectorService:
        def __init__(self):
            self.registry = ConnectorCapabilityRegistry()
            self.initialize_calls = 0

        def require_capability(self, connector_name, feature, trading_pair=None):
            self.registry.require(connector_name, feature, trading_pair)

        async def initialize_order_book(self, **kwargs):
            self.initialize_calls += 1
            return True

    connector_service = GuardedConnectorService()
    service = MarketDataService(connector_service)

    response = await add_trading_pair(
        AddTradingPairRequest(
            connector_name="manzana",
            trading_pair="BTC-USD",
        ),
        service,
    )

    CHECK.assertIs(response.success, True)
    CHECK.assertEqual(1, connector_service.initialize_calls)


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
    security_module = _module("hummingbot.client.config.security")
    if not hasattr(security_module, "Security"):
        security_module.Security = type("Security", (), {})
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
    file_system.fs_util = types.SimpleNamespace(
        path_exists=lambda path: False,
        read_file=lambda path: "",
        ensure_file_and_dump_text=lambda path, content: None,
        delete_file=lambda **kwargs: None,
    )
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


def test_final_b2_adjacent_routes_follow_shift_bootstrap_truth():
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
    unsupported_requests = (
        ("get", "/connectors/manzana/order-types", None),
        ("post", "/trading/master/manzana/orders/order-1/cancel", None),
        (
            "post",
            "/trading/orders/active",
            {"account_names": ["master"], "connector_names": ["manzana"]},
        ),
    )
    for method, path, body in unsupported_requests:
        response = getattr(client, method)(path, json=body) if body is not None else getattr(client, method)(path)
        CHECK.assertEqual(400, response.status_code, (path, response.text))
        CapabilityErrorResponse.model_validate(response.json())
    CHECK.assertEqual([], downstream.calls)

    supported_requests = (
        ("get", "/market-data/order-book/diagnostics/manzana", None),
        ("post", "/market-data/order-book/restart/manzana", None),
        (
            "post",
            "/market-data/trading-pair/remove",
            {"connector_name": "manzana", "trading_pair": "BTC-USD"},
        ),
    )
    for method, path, body in supported_requests:
        response = getattr(client, method)(path, json=body) if body is not None else getattr(client, method)(path)
        CHECK.assertEqual(200, response.status_code, (path, response.text))
    CHECK.assertEqual(["diagnostics", "restart", "remove_pair"], downstream.calls)

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
    for method, path, _ in unsupported_requests + supported_requests:
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
                {
                    "connector_name": "unknown",
                    "trading_pair": "BTC-USD",
                    "interval": "1m",
                    "start_time": 1,
                    "end_time": 2,
                },
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
                    json={
                        "connector_name": "generic",
                        "trading_pair": "BTCUSD",
                        "interval": "1m",
                        "start_time": 1,
                        "end_time": 2,
                    },
                ),
                client.post(
                    "/market-data/candles",
                    json={"connector_name": "generic", "trading_pair": "BTC-USD"},
                ),
                client.post(
                    "/market-data/historical-candles",
                    json={
                        "connector_name": "generic",
                        "trading_pair": "BTC-USD",
                        "interval": "1m",
                        "start_time": 1,
                        "end_time": 2,
                    },
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
    security_module = _module("hummingbot.client.config.security")
    if not hasattr(security_module, "Security"):
        security_module.Security = type("Security", (), {})
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
    file_system.fs_util = types.SimpleNamespace(
        path_exists=lambda path: False,
        read_file=lambda path: "",
        ensure_file_and_dump_text=lambda path, content: None,
        delete_file=lambda **kwargs: None,
    )
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


def _final_boundary_service(module, registry_type):
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
    service._connector_cleanup_phases = {}
    service._capability_registry = registry_type()
    service.db_manager = None
    return service


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation", ["bootstrap_loss", "incomplete_chain", "unknown_extra"]
)
async def test_final_declaration_boundary_rejects_concurrent_waiters_transactionally(
    mutation,
):
    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)
    symbol_map_entered = asyncio.Event()
    release_symbol_map = asyncio.Event()
    constructor_calls = 0
    post_symbol_calls = []

    class Tracker:
        def __init__(self):
            self._trading_pairs = []
            self.order_books = {}
            self._tracking_tasks = {}
            self.stop_calls = 0

        def stop(self):
            self.stop_calls += 1

    class Connector:
        in_flight_orders = {}

        def __init__(self):
            self.api_capabilities = _shift_successor_capabilities()
            self.order_book_tracker = Tracker()
            self._trading_pairs = []
            self.stop_calls = 0
            self.symbol_map_calls = 0

        async def _initialize_trading_pair_symbol_map(self):
            self.symbol_map_calls += 1
            symbol_map_entered.set()
            await release_symbol_map.wait()
            declaration = _shift_successor_capabilities(
                supported_pairs={"BTC-USD", "ETH-USD"}
            )
            if mutation == "bootstrap_loss":
                declaration["features"] = set(declaration["features"]) - {
                    module.ConnectorFeature.ORDER_BOOK
                }
            elif mutation == "incomplete_chain":
                declaration["features"] = {
                    *declaration["features"],
                    module.ConnectorFeature.ORDER_SUBMISSION,
                }
                self.buy = lambda **kwargs: "buy-id"
                self.sell = lambda **kwargs: "sell-id"
                self.cancel = None
            else:
                declaration["features"] = {
                    *declaration["features"],
                    "unknown_instance_feature",
                }
            self.api_capabilities = declaration

        async def _update_trading_rules(self):
            post_symbol_calls.append("rules")

        async def _update_balances(self):
            post_symbol_calls.append("balances")

        async def stop_network(self):
            self.stop_calls += 1
            self.order_book_tracker.stop()

    connector = Connector()

    def construct(account_name, connector_name):
        nonlocal constructor_calls
        constructor_calls += 1
        return connector

    service._create_trading_connector = construct
    service._initialize_metrics = lambda *args: post_symbol_calls.append("metrics")

    async def start_network(connector_arg):
        post_symbol_calls.append("network")

    service._start_connector_network = start_network
    first = asyncio.create_task(
        service.get_trading_connector("master", "manzana")
    )
    await symbol_map_entered.wait()
    second = asyncio.create_task(
        service.get_trading_connector("master", "manzana")
    )
    with pytest.raises(module.ConnectorCapabilityError) as active_root:
        await second
    CHECK.assertEqual(
        "market_transaction_retained",
        active_root.value.detail["cleanup_code"],
    )
    release_symbol_map.set()
    first_result = (await asyncio.gather(first, return_exceptions=True))[0]

    CHECK.assertIsInstance(first_result, module.ConnectorCapabilityError)
    CHECK.assertEqual("connector_capability_mismatch", first_result.detail["code"])
    CHECK.assertEqual(1, constructor_calls)
    CHECK.assertEqual(1, connector.symbol_map_calls)
    CHECK.assertEqual([], post_symbol_calls)
    CHECK.assertEqual(1, connector.order_book_tracker.stop_calls)
    CHECK.assertEqual(1, connector.stop_calls)
    CHECK.assertEqual([], connector._trading_pairs)
    CHECK.assertEqual([], connector.order_book_tracker._trading_pairs)
    CHECK.assertEqual({}, connector.order_book_tracker.order_books)
    CHECK.assertEqual({}, service._trading_connectors)
    CHECK.assertEqual({}, service._orders_recorders)
    CHECK.assertEqual({}, service._funding_recorders)
    CHECK.assertEqual({}, service._metrics_collectors)
    CHECK.assertEqual({}, service._connector_creation_residuals)
    CHECK.assertEqual({}, service._connector_cleanup_owners)
    CHECK.assertEqual({}, service._connector_cleanup_indices)
    CHECK.assertEqual({}, service._connector_residual_tasks)
    CHECK.assertEqual({}, service._connector_cleanup_phases)
    CHECK.assertEqual({}, service._connector_lock_users)
    CHECK.assertEqual({}, service._connector_locks)
    CHECK.assertEqual(
        {}, getattr(service, "_connector_creation_failures", {})
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("final_declaration", ["pair_narrowed", "validated_superset"])
async def test_final_declaration_boundary_publishes_compatible_truth(final_declaration):
    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)
    lifecycle_calls = []

    class Connector:
        order_book_tracker = None
        in_flight_orders = {}

        def __init__(self):
            self.api_capabilities = _shift_successor_capabilities()

        async def _initialize_trading_pair_symbol_map(self):
            declaration = _shift_successor_capabilities(
                supported_pairs={"BTC-USD", "ETH-USD"}
            )
            if final_declaration == "validated_superset":
                declaration["features"] = {
                    *declaration["features"],
                    module.ConnectorFeature.ORDER_SUBMISSION,
                }
                self.buy = lambda **kwargs: "buy-id"
                self.sell = lambda **kwargs: "sell-id"
                self.cancel = lambda **kwargs: "cancel-id"
            self.api_capabilities = declaration
            lifecycle_calls.append("symbol_map")

        async def _update_trading_rules(self):
            lifecycle_calls.append("rules")

        async def _update_balances(self):
            lifecycle_calls.append("balances")

        async def stop_network(self):
            lifecycle_calls.append("stop")

    connector = Connector()
    service._create_trading_connector = lambda account, name: connector
    service._initialize_metrics = lambda *args: lifecycle_calls.append("metrics")

    async def start_network(connector_arg):
        lifecycle_calls.append("network")

    service._start_connector_network = start_network
    loaded = await service.get_trading_connector("master", "manzana")

    CHECK.assertIs(connector, loaded)
    CHECK.assertIs(
        connector, service._trading_connectors["master"]["manzana"]
    )
    CHECK.assertEqual(
        ["symbol_map", "rules", "balances", "metrics", "network"],
        lifecycle_calls,
    )
    CHECK.assertEqual(
        frozenset({"BTC-USD", "ETH-USD"}),
        service.get_connector_capabilities("manzana", connector).supported_pairs,
    )
    CHECK.assertEqual({}, service._connector_lock_users)
    CHECK.assertEqual({}, service._connector_locks)


@pytest.mark.asyncio
async def test_final_declaration_boundary_rechecks_after_late_initialization():
    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)

    class Index:
        def __init__(self):
            self.stop_calls = 0

        async def stop(self):
            self.stop_calls += 1

    class Connector:
        order_book_tracker = None
        in_flight_orders = {}

        def __init__(self):
            self.api_capabilities = _shift_successor_capabilities()
            self.stop_calls = 0

        async def _initialize_trading_pair_symbol_map(self):
            self.api_capabilities = _shift_successor_capabilities(
                supported_pairs={"BTC-USD"}
            )

        async def _update_trading_rules(self):
            return None

        async def _update_balances(self):
            return None

        async def stop_network(self):
            self.stop_calls += 1

    connector = Connector()
    orders = Index()
    funding = Index()
    metrics = Index()
    service._create_trading_connector = lambda account, name: connector

    def initialize_indices(connector_arg, account_name, connector_name, cache_key):
        service._orders_recorders[cache_key] = orders
        service._funding_recorders[cache_key] = funding
        service._metrics_collectors[cache_key] = metrics

    service._initialize_metrics = initialize_indices

    async def mutate_after_network_start(connector_arg):
        declaration = _shift_successor_capabilities(
            supported_pairs={"BTC-USD"}
        )
        declaration["features"] = set(declaration["features"]) - {
            module.ConnectorFeature.LIVE_ORDER_BOOK
        }
        connector_arg.api_capabilities = declaration

    service._start_connector_network = mutate_after_network_start

    with pytest.raises(module.ConnectorCapabilityError) as mismatch:
        await service.get_trading_connector("master", "manzana")

    CHECK.assertEqual("connector_capability_mismatch", mismatch.value.detail["code"])
    CHECK.assertEqual(1, connector.stop_calls)
    CHECK.assertEqual((1, 1, 1), (orders.stop_calls, funding.stop_calls, metrics.stop_calls))
    CHECK.assertEqual({}, service._trading_connectors)
    CHECK.assertEqual({}, service._orders_recorders)
    CHECK.assertEqual({}, service._funding_recorders)
    CHECK.assertEqual({}, service._metrics_collectors)
    CHECK.assertEqual({}, service._connector_creation_residuals)
    CHECK.assertEqual({}, service._connector_cleanup_owners)
    CHECK.assertEqual({}, service._connector_cleanup_indices)
    CHECK.assertEqual({}, service._connector_residual_tasks)
    CHECK.assertEqual({}, service._connector_cleanup_phases)
    CHECK.assertEqual({}, service._connector_lock_users)
    CHECK.assertEqual({}, service._connector_locks)
    CHECK.assertEqual({}, service._connector_creation_failures)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation", ["bootstrap_loss", "incomplete_chain", "unknown_extra"]
)
async def test_public_symbol_discovery_revalidates_before_pair_mutation(mutation):
    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)
    pair_mutation_calls = []

    class Tracker:
        _trading_pairs = []
        order_books = {}

    class Connector:
        def __init__(self):
            self.api_capabilities = _shift_successor_capabilities()
            self._trading_pairs = []
            self.order_book_tracker = Tracker()

        async def exchange_symbol_associated_to_pair(self, trading_pair):
            declaration = _shift_successor_capabilities(
                supported_pairs={"BTC-USD"}
            )
            if mutation == "bootstrap_loss":
                declaration["features"] = set(declaration["features"]) - {
                    module.ConnectorFeature.ORDER_BOOK
                }
            elif mutation == "incomplete_chain":
                declaration["features"] = {
                    *declaration["features"],
                    module.ConnectorFeature.ORDER_SUBMISSION,
                }
                self.buy = lambda **kwargs: "buy-id"
                self.sell = lambda **kwargs: "sell-id"
                self.cancel = None
            else:
                declaration["features"] = {
                    *declaration["features"],
                    "unknown_instance_feature",
                }
            self.api_capabilities = declaration
            return "BTCUSD"

        async def stop_network(self):
            self.stop_calls += 1

    connector = Connector()
    connector.stop_calls = 0
    service._data_connectors = {"manzana": connector}
    service._data_connectors_started = {"manzana": True}

    async def mutate_pair(connector_arg, trading_pair):
        pair_mutation_calls.append(trading_pair)
        connector_arg._trading_pairs.append(trading_pair)
        connector_arg.order_book_tracker._trading_pairs.append(trading_pair)
        connector_arg.order_book_tracker.order_books[trading_pair] = object()
        return True

    service._add_trading_pair_to_tracker = mutate_pair
    service._wait_for_order_book = lambda *args: asyncio.sleep(0, result=True)

    with pytest.raises(module.ConnectorCapabilityError) as mismatch:
        await service.initialize_order_book("manzana", "BTC-USD")

    CHECK.assertEqual("connector_capability_mismatch", mismatch.value.detail["code"])
    CHECK.assertEqual([], pair_mutation_calls)
    CHECK.assertEqual([], connector._trading_pairs)
    CHECK.assertEqual([], connector.order_book_tracker._trading_pairs)
    CHECK.assertEqual({}, connector.order_book_tracker.order_books)
    CHECK.assertEqual(1, connector.stop_calls)
    CHECK.assertEqual({}, service._data_connectors)
    CHECK.assertEqual({}, service._data_connectors_started)
    CHECK.assertEqual({}, service._connector_cleanup_owners)
    CHECK.assertEqual({}, service._connector_residual_tasks)
    CHECK.assertEqual({}, service._connector_cleanup_phases)
    CHECK.assertEqual(
        "api-bootstrap",
        service.get_connector_capabilities("manzana").source,
    )


@pytest.mark.asyncio
async def test_network_cleanup_delegates_tracker_stop_to_connector_once():
    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)

    class Tracker:
        def __init__(self):
            self.stop_calls = 0

        def stop(self):
            self.stop_calls += 1

    class Connector:
        def __init__(self):
            self.order_book_tracker = Tracker()
            self.stop_calls = 0

        async def stop_network(self):
            self.stop_calls += 1
            self.order_book_tracker.stop()

    connector = Connector()

    await service._stop_connector_network(connector, "master", "manzana")

    CHECK.assertEqual(1, connector.stop_calls)
    CHECK.assertEqual(1, connector.order_book_tracker.stop_calls)
    CHECK.assertEqual({}, service._connector_cleanup_owners)
    CHECK.assertEqual({}, service._connector_residual_tasks)
    CHECK.assertEqual({}, service._connector_cleanup_phases)


@pytest.mark.asyncio
async def test_late_declaration_rejection_stops_fresh_candidate_once():
    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)

    class Tracker:
        def __init__(self):
            self.stop_calls = 0

        def stop(self):
            self.stop_calls += 1

    class Connector:
        in_flight_orders = {}

        def __init__(self):
            self.api_capabilities = _shift_successor_capabilities()
            self.order_book_tracker = Tracker()
            self.stop_calls = 0
            self.start_calls = 0

        async def _initialize_trading_pair_symbol_map(self):
            self.api_capabilities = _shift_successor_capabilities(
                supported_pairs={"BTC-USD"}
            )

        async def _update_trading_rules(self):
            return None

        async def _update_balances(self):
            return None

        async def start_network(self):
            self.start_calls += 1
            declaration = _shift_successor_capabilities(
                supported_pairs={"BTC-USD"}
            )
            declaration["features"] = set(declaration["features"]) - {
                module.ConnectorFeature.LIVE_ORDER_BOOK
            }
            self.api_capabilities = declaration

        async def stop_network(self):
            self.stop_calls += 1
            self.order_book_tracker.stop()

    connector = Connector()
    service._create_trading_connector = lambda account, name: connector
    service._initialize_metrics = lambda *args: None

    with pytest.raises(module.ConnectorCapabilityError) as mismatch:
        await service.get_trading_connector("master", "manzana")

    CHECK.assertEqual("connector_capability_mismatch", mismatch.value.detail["code"])
    CHECK.assertEqual(1, connector.start_calls)
    CHECK.assertEqual(1, connector.stop_calls)
    CHECK.assertEqual(1, connector.order_book_tracker.stop_calls)
    CHECK.assertEqual({}, service._trading_connectors)
    CHECK.assertEqual({}, service._connector_cleanup_owners)
    CHECK.assertEqual({}, service._connector_residual_tasks)
    CHECK.assertEqual({}, service._connector_cleanup_phases)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure_type", "preloaded"),
    [(KeyError, True), (RuntimeError, True), (RuntimeError, False)],
)
async def test_public_symbol_discovery_exception_is_typed_sanitized_and_fail_closed(
    failure_type,
    preloaded,
    caplog,
):
    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)
    pair_mutation_calls = []
    secret = "private-symbol-discovery-payload"

    class Tracker:
        def __init__(self):
            self._trading_pairs = []
            self.order_books = {}

    class Connector:
        def __init__(self):
            self.api_capabilities = _shift_successor_capabilities()
            self._trading_pairs = []
            self.order_book_tracker = Tracker()
            self.stop_calls = 0
            self.start_calls = 0

        async def exchange_symbol_associated_to_pair(self, trading_pair):
            raise failure_type(secret)

        async def start_network(self):
            self.start_calls += 1

        async def stop_network(self):
            self.stop_calls += 1

    connector = Connector()
    if preloaded:
        service._data_connectors = {"manzana": connector}
        service._data_connectors_started = {"manzana": True}
    else:
        service._create_data_connector = lambda connector_name: connector

    async def mutate_pair(connector_arg, trading_pair):
        pair_mutation_calls.append(trading_pair)
        return True

    service._add_trading_pair_to_tracker = mutate_pair

    with caplog.at_level("WARNING"):
        with pytest.raises(module.ConnectorCapabilityError) as failure:
            await service.initialize_order_book("manzana", "TYPO-USD", timeout=0)

    CHECK.assertEqual("connector_symbol_discovery_failed", failure.value.detail["code"])
    CHECK.assertNotIn(secret, json.dumps(failure.value.detail))
    CHECK.assertNotIn(secret, caplog.text)
    CHECK.assertEqual([], pair_mutation_calls)
    CHECK.assertEqual([], connector._trading_pairs)
    CHECK.assertEqual([], connector.order_book_tracker._trading_pairs)
    CHECK.assertEqual({}, connector.order_book_tracker.order_books)
    CHECK.assertEqual(0, connector.start_calls)
    CHECK.assertEqual(1, connector.stop_calls)
    CHECK.assertEqual({}, service._data_connectors)
    CHECK.assertEqual({}, service._data_connectors_started)
    CHECK.assertEqual({}, service._connector_cleanup_owners)
    CHECK.assertEqual({}, service._connector_residual_tasks)
    CHECK.assertEqual({}, service._connector_cleanup_phases)


@pytest.mark.asyncio
async def test_old_core_cleanup_calls_only_the_connector_boundary_once():
    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)

    class OldCoreTracker:
        def __init__(self):
            self.stop_calls = 0

        def stop(self):
            self.stop_calls += 1

        async def stop_and_wait(self):
            self.stop()

    class OldCoreConnector:
        def __init__(self):
            self.order_book_tracker = OldCoreTracker()
            self.stop_calls = 0

        async def stop_network(self):
            self.stop_calls += 1
            self.order_book_tracker.stop()
            await self.order_book_tracker.stop_and_wait()

    connector = OldCoreConnector()

    await service._stop_connector_network(connector, "master", "manzana")

    CHECK.assertEqual(1, connector.stop_calls)
    # Accepted core 0529e34 stops twice internally. A direct service tracker stop
    # would make this three, so this count proves ownership stays at the connector.
    CHECK.assertEqual(2, connector.order_book_tracker.stop_calls)
    CHECK.assertEqual({}, service._connector_cleanup_owners)
    CHECK.assertEqual({}, service._connector_residual_tasks)
    CHECK.assertEqual({}, service._connector_cleanup_phases)


def test_cached_capability_reads_revalidate_instance_only_callable_chains():
    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)
    declaration = _shift_successor_capabilities(supported_pairs={"BTC-USD"})
    declaration["features"] = {
        *declaration["features"],
        module.ConnectorFeature.ORDER_SUBMISSION,
    }
    connector = types.SimpleNamespace(
        api_capabilities=declaration,
        buy=lambda **kwargs: "buy-id",
        sell=lambda **kwargs: "sell-id",
        cancel=lambda **kwargs: "cancel-id",
    )
    service._data_connectors = {"manzana": connector}

    service.require_capability(
        "manzana", module.ConnectorFeature.ORDER_SUBMISSION
    )
    connector.cancel = None

    with pytest.raises(module.ConnectorCapabilityError) as mismatch:
        service.require_capability(
            "manzana", module.ConnectorFeature.ORDER_SUBMISSION
        )
    CHECK.assertEqual("connector_capability_mismatch", mismatch.value.detail["code"])


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_kind", ["getter", "mapping", "member"])
async def test_declaration_access_failures_are_typed_and_shared_by_waiters(
    failure_kind,
):
    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)
    symbol_map_entered = asyncio.Event()
    release_symbol_map = asyncio.Event()
    constructor_calls = 0
    secret = f"private-{failure_kind}-payload"

    class ExplodingMapping:
        def __init__(self, values):
            self.values = values

        def keys(self):
            raise RuntimeError(secret)

        def __iter__(self):
            raise RuntimeError(secret)

        def __getitem__(self, key):
            return self.values[key]

    class Tracker:
        def __init__(self):
            self.stop_calls = 0

        def stop(self):
            self.stop_calls += 1

    class Connector:
        in_flight_orders = {}

        def __init__(self):
            self._declaration = _shift_successor_capabilities()
            self._getter_fails = False
            self._member_fails = False
            self.order_book_tracker = Tracker()
            self.stop_calls = 0

        @property
        def api_capabilities(self):
            if self._getter_fails:
                raise RuntimeError(secret)
            return self._declaration

        @property
        def buy(self):
            if self._member_fails:
                raise RuntimeError(secret)
            return lambda **kwargs: "buy-id"

        async def _initialize_trading_pair_symbol_map(self):
            symbol_map_entered.set()
            await release_symbol_map.wait()
            if failure_kind == "getter":
                self._getter_fails = True
            elif failure_kind == "mapping":
                self._declaration = ExplodingMapping(
                    _shift_successor_capabilities(supported_pairs={"BTC-USD"})
                )
            else:
                self._member_fails = True
                declaration = _shift_successor_capabilities(
                    supported_pairs={"BTC-USD"}
                )
                declaration["features"] = {
                    *declaration["features"],
                    module.ConnectorFeature.ORDER_SUBMISSION,
                }
                self._declaration = declaration
                self.sell = lambda **kwargs: "sell-id"
                self.cancel = lambda **kwargs: "cancel-id"

        async def _update_trading_rules(self):
            raise AssertionError("invalid declaration reached trading rules")

        async def _update_balances(self):
            raise AssertionError("invalid declaration reached balances")

        async def stop_network(self):
            self.stop_calls += 1
            self.order_book_tracker.stop()

    connector = Connector()

    def construct(account_name, connector_name):
        nonlocal constructor_calls
        constructor_calls += 1
        return connector

    service._create_trading_connector = construct
    service._initialize_metrics = lambda *args: None
    first = asyncio.create_task(
        service.get_trading_connector("master", "manzana")
    )
    await symbol_map_entered.wait()
    second = asyncio.create_task(
        service.get_trading_connector("master", "manzana")
    )
    with pytest.raises(module.ConnectorCapabilityError) as active_root:
        await second
    release_symbol_map.set()
    first_result = (await asyncio.gather(first, return_exceptions=True))[0]
    results = [first_result, active_root.value]

    CHECK.assertTrue(
        all(isinstance(result, module.ConnectorCapabilityError) for result in results),
        results,
    )
    CHECK.assertEqual(
        ["connector_capability_mismatch", "connector_cleanup_residual"],
        [result.detail["code"] for result in results],
    )
    CHECK.assertTrue(all(secret not in json.dumps(result.detail) for result in results))
    CHECK.assertEqual(1, constructor_calls)
    CHECK.assertEqual(1, connector.stop_calls)
    CHECK.assertEqual(1, connector.order_book_tracker.stop_calls)
    CHECK.assertEqual({}, service._trading_connectors)
    CHECK.assertEqual({}, service._connector_creation_residuals)
    CHECK.assertEqual({}, service._connector_cleanup_owners)
    CHECK.assertEqual({}, service._connector_residual_tasks)
    CHECK.assertEqual({}, service._connector_cleanup_phases)
    CHECK.assertEqual({}, service._connector_lock_users)
    CHECK.assertEqual({}, service._connector_locks)
    CHECK.assertEqual({}, service._connector_creation_failures)


@pytest.mark.asyncio
async def test_public_symbol_discovery_accepts_compatible_final_declaration():
    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)
    pair_mutation_calls = []

    class Tracker:
        _trading_pairs = []
        order_books = {}

    class Connector:
        def __init__(self):
            self.api_capabilities = _shift_successor_capabilities()
            self._trading_pairs = []
            self.order_book_tracker = Tracker()

        async def exchange_symbol_associated_to_pair(self, trading_pair):
            self.api_capabilities = _shift_successor_capabilities(
                supported_pairs={"BTC-USD"}
            )
            return "BTCUSD"

    connector = Connector()
    service._data_connectors = {"manzana": connector}
    service._data_connectors_started = {"manzana": True}

    async def add_pair(connector_arg, trading_pair):
        pair_mutation_calls.append(trading_pair)
        return True

    service._add_trading_pair_to_tracker = add_pair
    service._wait_for_order_book = lambda *args: asyncio.sleep(0, result=True)

    ready = await service.initialize_order_book("manzana", "BTC-USD")

    CHECK.assertIs(ready, True)
    CHECK.assertEqual(["BTC-USD"], pair_mutation_calls)


@pytest.mark.asyncio
@pytest.mark.parametrize("preloaded", [False, True])
@pytest.mark.parametrize(
    "descriptor_kind", ["runtime", "attribute", "noncallable", "missing"]
)
async def test_public_symbol_discovery_descriptor_failures_are_closed_and_sanitized(
    descriptor_kind,
    preloaded,
    caplog,
):
    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)
    secret = f"private-{descriptor_kind}-descriptor-value"
    pair_mutations = []

    class Tracker:
        def __init__(self):
            self._trading_pairs = []
            self.order_books = {}

    class ExplodingDescriptor:
        def __get__(self, instance, owner):
            if descriptor_kind == "attribute":
                raise AttributeError(secret)
            raise RuntimeError(secret)

    class Connector:
        def __init__(self):
            self.api_capabilities = _shift_successor_capabilities()
            self._trading_pairs = []
            self.order_book_tracker = Tracker()
            self.stop_calls = 0
            self.start_calls = 0

        async def start_network(self):
            self.start_calls += 1

        async def stop_network(self):
            self.stop_calls += 1

    if descriptor_kind in {"runtime", "attribute"}:
        Connector.exchange_symbol_associated_to_pair = ExplodingDescriptor()
    elif descriptor_kind == "noncallable":
        Connector.exchange_symbol_associated_to_pair = property(lambda self: object())

    connector = Connector()
    if preloaded:
        service._data_connectors = {"manzana": connector}
        service._data_connectors_started = {"manzana": True}
    else:
        service._create_data_connector = lambda connector_name: connector

    async def mutate_pair(connector_arg, trading_pair):
        pair_mutations.append(trading_pair)
        return True

    service._add_trading_pair_to_tracker = mutate_pair
    service._wait_for_order_book = lambda *args: asyncio.sleep(0, result=True)

    with caplog.at_level("WARNING"):
        with pytest.raises(module.ConnectorCapabilityError) as failure:
            await service.initialize_order_book("manzana", "BTC-USD", timeout=0)

    CHECK.assertEqual(
        "connector_symbol_discovery_failed", failure.value.detail["code"]
    )
    CHECK.assertNotIn(secret, json.dumps(failure.value.detail))
    CHECK.assertNotIn(secret, caplog.text)
    CHECK.assertEqual([], pair_mutations)
    CHECK.assertEqual(0, connector.start_calls)
    CHECK.assertEqual(1, connector.stop_calls)
    CHECK.assertEqual({}, service._data_connectors)
    CHECK.assertEqual({}, service._data_connectors_started)


@pytest.mark.asyncio
async def test_preloaded_symbol_discovery_mutation_then_raise_never_stays_published():
    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)
    secret = "private-mutating-symbol-discovery-value"

    class Tracker:
        def __init__(self):
            self._trading_pairs = []
            self.order_books = {}

    class Connector:
        def __init__(self):
            self.api_capabilities = _shift_successor_capabilities()
            self._trading_pairs = []
            self.order_book_tracker = Tracker()
            self.stop_calls = 0

        async def exchange_symbol_associated_to_pair(self, trading_pair):
            declaration = _shift_successor_capabilities()
            declaration["features"] = set(declaration["features"]) - {
                module.ConnectorFeature.LIVE_ORDER_BOOK
            }
            self.api_capabilities = declaration
            self._trading_pairs.append("REJECTED-PAIR")
            raise RuntimeError(secret)

        async def stop_network(self):
            self.stop_calls += 1

    connector = Connector()
    service._data_connectors = {"manzana": connector}
    service._data_connectors_started = {"manzana": True}

    with pytest.raises(module.ConnectorCapabilityError) as failure:
        await service.initialize_order_book("manzana", "BTC-USD", timeout=0)

    CHECK.assertEqual(
        "connector_symbol_discovery_failed", failure.value.detail["code"]
    )
    CHECK.assertNotIn(secret, json.dumps(failure.value.detail))
    CHECK.assertEqual(1, connector.stop_calls)
    CHECK.assertEqual({}, service._data_connectors)
    CHECK.assertEqual({}, service._data_connectors_started)
    CHECK.assertEqual(
        "api-bootstrap", service.get_connector_capabilities("manzana").source
    )


@pytest.mark.asyncio
async def test_data_start_revalidates_final_declaration_before_started_or_success(
    monkeypatch,
):
    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)

    async def immediate_sleep(delay, result=None):
        return result

    monkeypatch.setattr(module.asyncio, "sleep", immediate_sleep)

    class ReadyBook:
        snapshot = ([1], [1])

    class Tracker:
        def __init__(self):
            self._trading_pairs = []
            self.order_books = {}
            self._order_book_stream_listener_task = object()

    class Connector:
        def __init__(self):
            self.api_capabilities = _shift_successor_capabilities()
            self._trading_pairs = []
            self.order_book_tracker = Tracker()
            self.start_calls = 0
            self.stop_calls = 0

        async def exchange_symbol_associated_to_pair(self, trading_pair):
            return "BTCUSD"

        async def start_network(self):
            self.start_calls += 1
            declaration = _shift_successor_capabilities(
                supported_pairs={"BTC-USD"}
            )
            declaration["features"] = set(declaration["features"]) - {
                module.ConnectorFeature.LIVE_ORDER_BOOK
            }
            self.api_capabilities = declaration
            self.order_book_tracker.order_books["BTC-USD"] = ReadyBook()

        async def stop_network(self):
            self.stop_calls += 1

    connector = Connector()
    service._create_data_connector = lambda connector_name: connector

    with pytest.raises(module.ConnectorCapabilityError) as mismatch:
        await service.initialize_order_book("manzana", "BTC-USD", timeout=0)

    CHECK.assertEqual("connector_capability_mismatch", mismatch.value.detail["code"])
    CHECK.assertEqual(1, connector.start_calls)
    CHECK.assertEqual(1, connector.stop_calls)
    CHECK.assertEqual({}, service._data_connectors)
    CHECK.assertEqual({}, service._data_connectors_started)


@pytest.mark.asyncio
async def test_public_data_initialization_serializes_paused_a_before_rejecting_b():
    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)
    a_paused = asyncio.Event()
    release_a = asyncio.Event()
    events = []

    class Tracker:
        def __init__(self):
            self._trading_pairs = []
            self.order_books = {}

    class Connector:
        def __init__(self):
            self.api_capabilities = _shift_successor_capabilities()
            self._trading_pairs = []
            self.order_book_tracker = Tracker()
            self.discovery_calls = 0
            self.stop_calls = 0

        async def exchange_symbol_associated_to_pair(self, trading_pair):
            self.discovery_calls += 1
            if self.discovery_calls == 1:
                events.append("a_discovery")
                return "AUSD"
            events.append("b_discovery")
            declaration = _shift_successor_capabilities()
            declaration["features"] = set(declaration["features"]) - {
                module.ConnectorFeature.LIVE_ORDER_BOOK
            }
            self.api_capabilities = declaration
            return "BUSD"

        async def stop_network(self):
            self.stop_calls += 1

    connector = Connector()
    service._data_connectors = {"manzana": connector}
    service._data_connectors_started = {"manzana": True}

    async def add_pair(connector_arg, trading_pair):
        events.append("a_paused")
        a_paused.set()
        await release_a.wait()
        events.append("a_pair_mutation")
        connector_arg._trading_pairs.append(trading_pair)
        return True

    async def wait_for_book(tracker, trading_pair, timeout):
        events.append(
            "a_return_owned"
            if service._data_connectors.get("manzana") is connector
            else "a_return_unowned"
        )
        return True

    service._add_trading_pair_to_tracker = add_pair
    service._wait_for_order_book = wait_for_book

    request_a = asyncio.create_task(
        service.initialize_order_book("manzana", "A-USD", timeout=0)
    )
    await a_paused.wait()
    request_b = asyncio.create_task(
        service.initialize_order_book("manzana", "B-USD", timeout=0)
    )
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    release_a.set()
    result_a, result_b = await asyncio.gather(
        request_a, request_b, return_exceptions=True
    )

    CHECK.assertIs(result_a, True)
    CHECK.assertIsInstance(result_b, module.ConnectorCapabilityError)
    CHECK.assertEqual("connector_cleanup_residual", result_b.detail["code"])
    CHECK.assertEqual(
        "market_transaction_retained", result_b.detail["cleanup_code"]
    )
    CHECK.assertNotIn("b_discovery", events)
    CHECK.assertIn("a_return_owned", events)
    CHECK.assertNotIn("a_return_unowned", events)
    CHECK.assertEqual(0, connector.stop_calls)
    CHECK.assertIs(connector, service._data_connectors["manzana"])
    CHECK.assertIs(service._data_connectors_started["manzana"], True)


@pytest.mark.asyncio
@pytest.mark.parametrize("preloaded", [False, True])
@pytest.mark.parametrize("boundary", ["discovery", "start", "book_wait"])
@pytest.mark.parametrize("outcome", ["cancel", "false", "raise"])
async def test_public_transaction_rolls_back_every_adverse_boundary(
    preloaded,
    boundary,
    outcome,
):
    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)
    entered = asyncio.Event()
    secret = f"private-{boundary}-{outcome}-value"
    spawned_tasks = []

    class ReadyBook:
        snapshot = ([1], [1])

    class Tracker:
        def __init__(self):
            self._trading_pairs = ["BASE-USD"]
            self.order_books = {"BASE-USD": ReadyBook()}
            self._tracking_tasks = {}
            self._order_book_stream_listener_task = None

    class Connector:
        def __init__(self):
            self.api_capabilities = _shift_successor_capabilities()
            self._trading_pairs = ["BASE-USD"]
            self.order_book_tracker = Tracker()
            self.stop_calls = 0

        async def exchange_symbol_associated_to_pair(self, trading_pair):
            return "BTCUSD"

        async def start_network(self):
            if boundary == "start":
                return await adverse_boundary()
            self.order_book_tracker._order_book_stream_listener_task = object()
            return True

        async def stop_network(self):
            self.stop_calls += 1

    connector = Connector()
    original_declaration = connector.api_capabilities
    original_capabilities = _shift_successor_capabilities()
    original_connector_pairs = ["BASE-USD"]
    original_tracker_pairs = ["BASE-USD"]
    original_books = dict(connector.order_book_tracker.order_books)

    async def adverse_boundary(*args, **kwargs):
        declaration = _shift_successor_capabilities()
        declaration["features"] = set(declaration["features"]) - {
            module.ConnectorFeature.LIVE_ORDER_BOOK
        }
        connector.api_capabilities = declaration
        connector._trading_pairs.append("DIRTY-PAIR")
        connector.order_book_tracker._trading_pairs.append("DIRTY-PAIR")
        connector.order_book_tracker.order_books["DIRTY-PAIR"] = object()
        spawned = asyncio.create_task(asyncio.Event().wait())
        spawned_tasks.append(spawned)
        connector.order_book_tracker._tracking_tasks["DIRTY-PAIR"] = spawned
        if outcome == "cancel":
            entered.set()
            await asyncio.Event().wait()
        if outcome == "raise":
            raise RuntimeError(secret)
        return False

    if preloaded:
        service._data_connectors = {"manzana": connector}
    else:
        service._create_data_connector = lambda connector_name: connector

    if boundary == "discovery":
        service._is_trading_pair_supported = adverse_boundary
        service._data_connectors_started = {"manzana": True} if preloaded else {}
    elif boundary == "start":
        service._is_trading_pair_supported = lambda *args: asyncio.sleep(
            0, result=True
        )
        service._data_connectors_started = {}
    else:
        service._is_trading_pair_supported = lambda *args: asyncio.sleep(
            0, result=True
        )
        service._data_connectors_started = {"manzana": True} if preloaded else {}

        async def first_touch_start(*args):
            service._data_connectors_started["manzana"] = True
            return True

        if not preloaded:
            service._ensure_data_connector_started_locked = first_touch_start
        service._add_trading_pair_to_tracker = lambda *args: asyncio.sleep(
            0, result=True
        )
        service._wait_for_order_book = adverse_boundary

    if outcome == "cancel":
        task = asyncio.create_task(
            service.initialize_order_book("manzana", "BTC-USD", timeout=0)
        )
        await entered.wait()
        task.cancel("public-transaction-cancel")
        with pytest.raises(asyncio.CancelledError) as cancelled:
            await task
        CHECK.assertEqual(("public-transaction-cancel",), cancelled.value.args)
    else:
        with pytest.raises(module.ConnectorCapabilityError) as failure:
            await service.initialize_order_book("manzana", "BTC-USD", timeout=0)
        CHECK.assertNotIn(secret, json.dumps(failure.value.detail))
        if boundary == "book_wait" and outcome == "raise":
            CHECK.assertEqual(
                "connector_order_book_wait_failed", failure.value.detail["code"]
            )

    CHECK.assertEqual(1, connector.stop_calls)
    CHECK.assertEqual({}, service._data_connectors)
    CHECK.assertEqual({}, service._data_connectors_started)
    CHECK.assertEqual({}, service._connector_incarnations)
    CHECK.assertIsNot(original_declaration, connector.api_capabilities)
    CHECK.assertNotEqual(original_capabilities, connector.api_capabilities)
    CHECK.assertEqual(
        [
            *original_connector_pairs,
            *(["BTC-USD"] if boundary == "start" else []),
            "DIRTY-PAIR",
        ],
        connector._trading_pairs,
    )
    CHECK.assertEqual(
        [*original_tracker_pairs, "DIRTY-PAIR"],
        connector.order_book_tracker._trading_pairs,
    )
    CHECK.assertEqual(
        {*original_books, "DIRTY-PAIR"},
        set(connector.order_book_tracker.order_books),
    )
    CHECK.assertIs(
        spawned_tasks[0],
        connector.order_book_tracker._tracking_tasks["DIRTY-PAIR"],
    )
    CHECK.assertTrue(spawned_tasks)
    if boundary == "start":
        CHECK.assertTrue(all(task.done() for task in spawned_tasks))
        CHECK.assertTrue(all(task.cancelled() for task in spawned_tasks))
    else:
        CHECK.assertTrue(all(not task.done() for task in spawned_tasks))
    CHECK.assertIn("DIRTY-PAIR", connector._trading_pairs)
    CHECK.assertEqual({}, service._connector_cleanup_owners)
    CHECK.assertEqual({}, service._connector_residual_tasks)
    CHECK.assertEqual({}, service._connector_cleanup_phases)
    for spawned in spawned_tasks:
        if not spawned.done():
            spawned.cancel()
    await asyncio.gather(*spawned_tasks, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["start", "pair"])
@pytest.mark.parametrize("outcome", ["cancel", "false", "raise"])
async def test_public_transaction_restores_exact_preloaded_owned_deltas(
    boundary,
    outcome,
):
    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)
    entered = asyncio.Event()
    spawned_tasks = []

    class ReadyBook:
        snapshot = ([1], [1])

    class Tracker:
        def __init__(self):
            self._trading_pairs = ["BASE-USD"]
            self.order_books = {"BASE-USD": ReadyBook()}
            self._tracking_tasks = {}

    class Connector:
        def __init__(self):
            self.api_capabilities = _shift_successor_capabilities()
            self._trading_pairs = ["BASE-USD"]
            self.order_book_tracker = Tracker()
            self.stop_calls = 0

        async def exchange_symbol_associated_to_pair(self, trading_pair):
            return "BTCUSD"

        async def start_network(self):
            return await mutate_then_finish(
                self.order_book_tracker, "BTC-USD", 0
            )

        async def stop_network(self):
            self.stop_calls += 1

    connector = Connector()
    original_declaration = connector.api_capabilities
    service._data_connectors = {"manzana": connector}
    original_incarnation = service._advance_connector_incarnation(
        ("__data__", "manzana")
    )
    service._data_connectors_started = (
        {"manzana": True} if boundary == "pair" else {}
    )
    service._add_trading_pair_to_tracker = lambda *args: asyncio.sleep(
        0, result=True
    )

    async def mutate_then_finish(tracker, trading_pair, timeout):
        if trading_pair not in connector._trading_pairs:
            connector._trading_pairs.append(trading_pair)
        if trading_pair not in tracker._trading_pairs:
            tracker._trading_pairs.append(trading_pair)
        tracker.order_books[trading_pair] = ReadyBook()
        spawned = asyncio.create_task(asyncio.Event().wait())
        spawned_tasks.append(spawned)
        tracker._tracking_tasks[trading_pair] = spawned
        if outcome == "cancel":
            entered.set()
            await asyncio.Event().wait()
        if outcome == "raise":
            raise RuntimeError("private-preloaded-rollback-value")
        return False

    if boundary == "pair":
        service._add_trading_pair_to_tracker = lambda connector_arg, pair: (
            mutate_then_finish(connector_arg.order_book_tracker, pair, 0)
        )
        service._wait_for_order_book = lambda *args: asyncio.sleep(0, result=True)
    if outcome == "cancel":
        task = asyncio.create_task(
            service.initialize_order_book("manzana", "BTC-USD", timeout=0)
        )
        await entered.wait()
        task.cancel(("retain-my-exact-args", 224))
        with pytest.raises(asyncio.CancelledError) as cancelled:
            await task
        CHECK.assertEqual((("retain-my-exact-args", 224),), cancelled.value.args)
    elif boundary == "pair" and outcome == "raise":
        with pytest.raises(module.ConnectorCapabilityError) as failure:
            await service.initialize_order_book("manzana", "BTC-USD", timeout=0)
        CHECK.assertEqual(
            "connector_pair_registration_failed", failure.value.detail["code"]
        )
        CHECK.assertNotIn(
            "private-preloaded-rollback-value", json.dumps(failure.value.detail)
        )
    else:
        CHECK.assertIs(
            await service.initialize_order_book("manzana", "BTC-USD", timeout=0),
            False,
        )

    CHECK.assertIs(connector, service._data_connectors["manzana"])
    CHECK.assertEqual(
        original_incarnation,
        service._connector_incarnations[("__data__", "manzana")],
    )
    if boundary == "pair":
        CHECK.assertIs(service._data_connectors_started["manzana"], True)
    else:
        CHECK.assertNotIn("manzana", service._data_connectors_started)
    CHECK.assertEqual(0, connector.stop_calls)
    CHECK.assertIs(original_declaration, connector.api_capabilities)
    CHECK.assertEqual(["BASE-USD"], connector._trading_pairs)
    CHECK.assertEqual(["BASE-USD"], connector.order_book_tracker._trading_pairs)
    CHECK.assertEqual(["BASE-USD"], list(connector.order_book_tracker.order_books))
    CHECK.assertEqual({}, connector.order_book_tracker._tracking_tasks)
    CHECK.assertTrue(spawned_tasks[0].done())
    CHECK.assertTrue(spawned_tasks[0].cancelled())


@pytest.mark.asyncio
async def test_public_transaction_cancellation_retains_failed_cleanup_residual():
    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)
    entered = asyncio.Event()

    class Tracker:
        def __init__(self):
            self._trading_pairs = []
            self.order_books = {}

    class Connector:
        def __init__(self):
            self.api_capabilities = _shift_successor_capabilities()
            self._trading_pairs = []
            self.order_book_tracker = Tracker()

        async def exchange_symbol_associated_to_pair(self, trading_pair):
            declaration = _shift_successor_capabilities()
            declaration["features"] = set(declaration["features"]) - {
                module.ConnectorFeature.LIVE_ORDER_BOOK
            }
            self.api_capabilities = declaration
            entered.set()
            await asyncio.Event().wait()

        async def stop_network(self):
            raise RuntimeError("private-stop-failure")

    connector = Connector()
    service._create_data_connector = lambda connector_name: connector
    task = asyncio.create_task(
        service.initialize_order_book("manzana", "BTC-USD", timeout=0)
    )
    await entered.wait()
    task.cancel("cleanup-must-not-eat-this")
    with pytest.raises(asyncio.CancelledError) as cancelled:
        await task

    CHECK.assertEqual(("cleanup-must-not-eat-this",), cancelled.value.args)
    CHECK.assertIs(connector, service._data_connectors["manzana"])
    residual = service._connector_creation_residuals[("__data__", "manzana")]
    CHECK.assertEqual("connector_capability_cleanup_residual", residual["code"])
    CHECK.assertNotIn("private-stop-failure", json.dumps(residual))
    CHECK.assertEqual(
        {("__data__", "manzana")}, set(service._connector_incarnations)
    )


@pytest.mark.asyncio
async def test_incarnation_registry_is_bounded_across_one_hundred_owner_cycles():
    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)
    connectors = []

    class Connector:
        order_book_tracker = None

        def __init__(self):
            self.api_capabilities = _shift_successor_capabilities()
            self.stop_calls = 0

        async def stop_network(self):
            self.stop_calls += 1

    def create(connector_name):
        connector = Connector()
        connectors.append(connector)
        return connector

    service._create_data_connector = create
    for _ in range(100):
        service.get_data_connector("manzana")
        CHECK.assertLessEqual(len(service._connector_incarnations), 1)
        await service.stop_all()
        CHECK.assertEqual({}, service._connector_incarnations)

    CHECK.assertEqual(100, len(connectors))
    CHECK.assertTrue(all(connector.stop_calls == 1 for connector in connectors))
    CHECK.assertGreaterEqual(service._connector_incarnation_sequence, 100)
    CHECK.assertEqual({}, service._connector_locks)
    CHECK.assertEqual({}, service._connector_lock_users)


@pytest.mark.asyncio
async def test_pruned_incarnation_token_cannot_alias_a_replacement_owner():
    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)

    class Connector:
        order_book_tracker = None

        def __init__(self):
            self.api_capabilities = _shift_successor_capabilities()

        async def stop_network(self):
            return None

    old = Connector()
    replacements = [old, Connector()]
    service._create_data_connector = lambda connector_name: replacements.pop(0)
    service.get_data_connector("manzana")
    old_capabilities = service._validate_instance_capabilities("manzana", old)
    stale_snapshot = service._snapshot_public_connector_state(
        "manzana",
        old,
        ("__data__", "manzana"),
        old_capabilities,
    )
    stale_token = stale_snapshot["incarnation"]

    await service.stop_all()
    CHECK.assertEqual({}, service._connector_incarnations)
    replacement = service.get_data_connector("manzana")
    replacement_token = service._connector_incarnations[("__data__", "manzana")]

    CHECK.assertIsNot(old, replacement)
    CHECK.assertNotEqual(stale_token, replacement_token)
    with pytest.raises(module.ConnectorCapabilityError) as changed:
        service._require_public_connector_ownership(stale_snapshot)
    CHECK.assertEqual("connector_lifecycle_changed", changed.value.detail["code"])


@pytest.mark.asyncio
async def test_data_network_timeout_owner_completes_exact_task_on_stop_all_retry():
    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)
    release_stop = asyncio.Event()

    class Connector:
        order_book_tracker = None

        def __init__(self):
            self.api_capabilities = _shift_successor_capabilities()
            self.stop_calls = 0

        async def stop_network(self):
            self.stop_calls += 1
            try:
                await release_stop.wait()
            except asyncio.CancelledError:
                await release_stop.wait()

    connector = Connector()
    service._data_connectors = {"manzana": connector}
    service._data_connectors_started = {"manzana": True}
    approved = service._validate_instance_capabilities("manzana", connector)

    with pytest.raises(module.ConnectorCapabilityError) as residual:
        await service._discard_invalid_public_declaration(
            "manzana", connector, approved
        )

    CHECK.assertEqual(
        "connector_capability_cleanup_residual", residual.value.detail["code"]
    )
    CHECK.assertEqual(1, connector.stop_calls)
    CHECK.assertIs(connector, service._data_connectors["manzana"])
    owner_key = ("__data__", "manzana", "data_network")
    retained_task = service._connector_cleanup_phases[owner_key]["connector_stop"][
        "task"
    ]

    release_stop.set()
    await retained_task
    await service.stop_all()
    await service.stop_all()

    CHECK.assertEqual(1, connector.stop_calls)
    CHECK.assertEqual({}, service._connector_creation_residuals)
    CHECK.assertEqual({}, service._connector_cleanup_owners)
    CHECK.assertEqual({}, service._connector_residual_tasks)
    CHECK.assertEqual({}, service._connector_cleanup_phases)


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal_kind", ["false", "raise"])
async def test_data_network_terminal_stop_failure_remains_truthful(terminal_kind):
    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)

    class Connector:
        order_book_tracker = None

        def __init__(self):
            self.api_capabilities = _shift_successor_capabilities()

        async def stop_network(self):
            if terminal_kind == "false":
                return False
            raise RuntimeError("private-stop-error")

    connector = Connector()
    service._data_connectors = {"manzana": connector}
    service._data_connectors_started = {"manzana": True}
    approved = service._validate_instance_capabilities("manzana", connector)

    with pytest.raises(module.ConnectorCapabilityError):
        await service._discard_invalid_public_declaration(
            "manzana", connector, approved
        )

    for _ in range(2):
        with pytest.raises(module.ConnectorCapabilityError) as residual:
            await service.stop_all()
        CHECK.assertEqual("connector_cleanup_residual", residual.value.detail["code"])
        expected_code = {
            "false": "connector_stop_returned_false",
            "raise": "connector_stop_raised",
        }[terminal_kind]
        CHECK.assertEqual(expected_code, residual.value.detail["cleanup_code"])

    owner_key = ("__data__", "manzana", "data_network")
    CHECK.assertIn(owner_key, service._connector_cleanup_owners)
    CHECK.assertIn(owner_key, service._connector_cleanup_phases)


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
    selected = ("deps", "models", "services", "database")
    original_modules = {name: sys.modules.get(name) for name in selected}
    sentinels = {}
    before_dicts = {}
    try:
        for name in selected:
            module = types.ModuleType(name)
            module.__path__ = (
                [str(Path(__file__).resolve().parents[1] / name)]
                if name != "deps"
                else []
            )
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


def test_b9_fixture_database_shim_does_not_poison_real_import_order():
    """A capability test may create database shims without poisoning later imports."""
    repo = Path(__file__).resolve().parents[1]
    script = r"""
import importlib
import pathlib
import sys

import pytest

root = pathlib.Path(sys.argv[1]).resolve()
node = (
    "test/test_issue222_manzana_capabilities.py::"
    "test_closure_b2_accounts_active_orders_gate_before_connector_lookup"
)
result = pytest.main(["-q", "-p", "no:cacheprovider", node])
if result != pytest.ExitCode.OK:
    raise SystemExit(f"capability fixture probe failed: {result}")

database = importlib.import_module("database")
repositories = importlib.import_module("database.repositories")
for module in (database, repositories):
    module_file = getattr(module, "__file__", None)
    if module_file is None or not pathlib.Path(module_file).resolve().is_relative_to(root):
        raise SystemExit(f"wrong database module provenance: {module_file}")
    if getattr(module, "__issue222_contract_shim__", False):
        raise SystemExit("capability database shim leaked after fixture cleanup")
if not hasattr(database, "__path__"):
    raise SystemExit("real database package was replaced by a non-package shim")
print(f"DATABASE_REPOSITORIES={repositories.__file__}")
"""
    env = os.environ.copy()
    env.pop("PYTHONPATH", None)
    env.pop("PYTHONHOME", None)
    env["PYTHONNOUSERSITE"] = "1"
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env["PYTHONHASHSEED"] = "22209"
    command = [sys.executable, "-B"]
    if sys.flags.optimize:
        command.append("-O")
    command.extend(("-c", script, str(repo)))

    result = subprocess.run(
        command,
        cwd=repo,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )

    CHECK.assertEqual(
        0,
        result.returncode,
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}",
    )
    CHECK.assertIn("DATABASE_REPOSITORIES=", result.stdout)


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
        calls.append("remove_pair")
        return True

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
    interface._register_market_owner("manzana", "BTC-USD")
    removal = await interface.remove_market("manzana", "BTC-USD")

    CHECK.assertEqual(["remove_pair"], calls)
    CHECK.assertIsNone(removal)
    CHECK.assertEqual({}, interface.markets)


@pytest.mark.asyncio
async def test_closure_b3_public_service_seams_gate_before_resolution():
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
    CHECK.assertEqual(
        {"reached": True},
        market_data.get_order_book_tracker_diagnostics("manzana"),
    )
    CHECK.assertEqual(
        {"reached": True},
        await market_data.restart_order_book_tracker("manzana"),
    )

    module, registry_type = _load_unified_service_module()
    unified = object.__new__(module.UnifiedConnectorService)
    unified._capability_registry = registry_type()

    class StaticConnector:
        api_capabilities = _shift_successor_capabilities()
        order_book_tracker = None

    def resolve(*args, **kwargs):
        calls.append("resolve")
        return StaticConnector()

    unified.get_best_connector_for_market = resolve
    CHECK.assertIs(
        await unified.initialize_order_book("manzana", "BTC-USD"), True
    )
    CHECK.assertIs(await unified.remove_trading_pair("manzana", "BTC-USD"), False)

    CHECK.assertEqual(["diagnostics", "restart", "resolve", "resolve"], calls)


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
        with pytest.raises(module.ConnectorCapabilityError) as active_root:
            await stopping
        CHECK.assertEqual(
            "market_transaction_retained",
            active_root.value.detail["cleanup_code"],
        )
    finally:
        creation_release.set()
        await creation
    CHECK.assertIs(connector, creation.result())
    CHECK.assertIs(
        True,
        await service.stop_trading_connector("master", "generic"),
    )

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

    with pytest.raises(module.ConnectorCapabilityError) as second:
        await service.restart_order_book_tracker("generic", "master")
    CHECK.assertEqual("connector_cleanup_residual", second.value.detail["code"])
    CHECK.assertEqual(
        "restart_cleanup_incomplete", second.value.detail["cleanup_code"]
    )
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

        def validate_order_book_ready(self, connector, trading_pair):
            if self.fail:
                raise ValueError("invalid pair")
            return True

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

    class Connector:
        order_book_tracker = tracker

        async def stop_network(self):
            await tracker.stop()

    connector = Connector()
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
    for _ in range(100):
        if not service._market_locks:
            break
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
    with pytest.raises(module.ConnectorCapabilityError) as retry:
        await service.restart_order_book_tracker("generic", "master")
    calls_before_release = tracker.stop_calls
    release.set()
    await asyncio.gather(*stop_tasks, return_exceptions=True)

    CHECK.assertEqual("connector_cleanup_residual", retry.value.detail["code"])
    CHECK.assertEqual(
        "restart_cleanup_incomplete", retry.value.detail["cleanup_code"]
    )
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

        def __init__(self):
            tracker = types.SimpleNamespace(
                _trading_pairs=["BTC-USD"],
                order_books={},
            )
            connector = types.SimpleNamespace(
                _trading_pairs=["BTC-USD"],
                order_book_tracker=tracker,
            )
            self._connector_service = types.SimpleNamespace(
                _trading_connectors={"master": {"generic": connector}},
                _data_connectors={},
            )

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

    class Connector:
        order_book_tracker = tracker

        async def stop_network(self):
            await tracker.stop()

    connector = Connector()
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

    CHECK.assertIs(True, results[0]["success"])
    CHECK.assertIsInstance(results[1], module.ConnectorCapabilityError)
    CHECK.assertEqual(
        "restart_cleanup_incomplete", results[1].detail["cleanup_code"]
    )
    CHECK.assertEqual(1, tracker.max_active_stops)
    CHECK.assertEqual(1, tracker.stop_calls)

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

    class Connector:
        order_book_tracker = tracker

        async def stop_network(self):
            tracker.stop()

    connector = Connector()
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
async def test_b9_data_restart_and_stop_all_coalesce_final_tracker_stop():
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

    class DataConnector:
        def __init__(self, tracker):
            self.order_book_tracker = tracker
            self.stop_network_calls = 0

        async def stop_network(self):
            self.stop_network_calls += 1
            self.order_book_tracker.stop()

    tracker = Tracker()
    connector = DataConnector(tracker)
    service._data_connectors = {"generic": connector}
    service._data_connectors_started = {"generic": True}
    service._wait_for_websocket_ready = lambda *args, **kwargs: asyncio.sleep(
        0, result=False
    )

    restart = await service.restart_order_book_tracker("generic")
    CHECK.assertEqual("tracker_websocket_not_ready", restart["error"])
    await service.stop_all()

    CHECK.assertEqual(1, connector.stop_network_calls)
    CHECK.assertEqual(2, tracker.stop_calls)
    CHECK.assertEqual({}, service._connector_cleanup_owners)
    CHECK.assertEqual({}, service._connector_cleanup_phases)
    CHECK.assertEqual({}, service._connector_residual_tasks)
    CHECK.assertEqual({}, service._connector_locks)


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

    class Connector:
        order_book_tracker = tracker

        async def stop_network(self):
            tracker.stop()

    connector = Connector()
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
    with pytest.raises(module.ConnectorCapabilityError) as blocked:
        await shutdown
    CHECK.assertEqual("connector_cleanup_residual", blocked.value.detail["code"])
    CHECK.assertEqual(
        "restart_cleanup_incomplete", blocked.value.detail["cleanup_code"]
    )
    release_start.set()
    await start_task
    for _ in range(100):
        if not service._market_locks:
            break
        await asyncio.sleep(0)
    await service.stop_all()

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
async def test_b9_hidden_owner_websocket_replacement_is_typed_and_nonmutating():
    from models.connectors import CapabilityErrorDetail
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

    hidden_feed = {
        "pairs": ["BTC-USD"],
        "books": {"BTC-USD": object()},
        "tasks": {"BTC-USD": object()},
    }
    initialize_calls = []

    class HiddenOwnerService:
        registry = ConnectorCapabilityRegistry()

        def require_capability(self, connector_name, feature, trading_pair=None):
            self.registry.require(connector_name, feature, trading_pair)

        async def initialize_order_book(self, connector_name, trading_pair):
            initialize_calls.append((connector_name, trading_pair))
            hidden_feed["pairs"].clear()
            hidden_feed["books"].clear()
            hidden_feed["tasks"].clear()
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
    manager = module.WebSocketManager(HiddenOwnerService())
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
    CHECK.assertEqual([], initialize_calls)
    CHECK.assertEqual(["BTC-USD"], hidden_feed["pairs"])
    CHECK.assertEqual(["BTC-USD"], list(hidden_feed["books"]))
    CHECK.assertEqual(["BTC-USD"], list(hidden_feed["tasks"]))
    detail = websocket.payloads[-1]["detail"]
    CapabilityErrorDetail.model_validate(detail)
    CHECK.assertEqual("replacement_validation_unavailable", detail["code"])
    CHECK.assertEqual("generic", detail["connector"])
    CHECK.assertEqual("BTC-USD", detail["trading_pair"])
    manager.shutdown()


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


class Issue224RepairBook:
    snapshot = ([1], [1])


class Issue224RepairTracker:
    def __init__(self):
        self._trading_pairs = ["BASE-USD"]
        self.order_books = {"BASE-USD": Issue224RepairBook()}
        self._tracking_tasks = {}
        self._in_flight_transactions = set()
        self._order_book_stream_listener_task = object()


class Issue224RepairConnector:
    def __init__(self, tracker=None):
        self.api_capabilities = _shift_successor_capabilities()
        self._trading_pairs = ["BASE-USD"]
        self.order_book_tracker = tracker or Issue224RepairTracker()
        self.stop_calls = 0

    async def exchange_symbol_associated_to_pair(self, trading_pair):
        return trading_pair.replace("-", "")

    async def stop_network(self):
        self.stop_calls += 1


def _issue224_repair_service():
    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)
    tracker = Issue224RepairTracker()
    connector = Issue224RepairConnector(tracker)
    service._data_connectors = {"manzana": connector}
    service._data_connectors_started = {"manzana": True}
    return module, service, connector, tracker


@pytest.mark.asyncio
async def test_issue224_owned_delta_rollback_preserves_concurrent_unrelated_state():
    _, service, connector, tracker = _issue224_repair_service()
    pair_mutated = asyncio.Event()
    release_pair = asyncio.Event()

    async def add_pair(connector_arg, trading_pair):
        connector_arg._trading_pairs.append(trading_pair)
        tracker._trading_pairs.append(trading_pair)
        tracker.order_books[trading_pair] = Issue224RepairBook()
        pair_mutated.set()
        await release_pair.wait()
        return False

    service._add_trading_pair_to_tracker = add_pair
    request = asyncio.create_task(
        service.initialize_order_book("manzana", "BTC-USD", timeout=0)
    )
    await pair_mutated.wait()
    connector._trading_pairs.append("ETH-USD")
    tracker._trading_pairs.append("ETH-USD")
    eth_book = Issue224RepairBook()
    tracker.order_books["ETH-USD"] = eth_book
    release_pair.set()

    CHECK.assertIs(await request, False)
    CHECK.assertIs(connector, service._data_connectors["manzana"])
    CHECK.assertEqual(["BASE-USD", "ETH-USD"], connector._trading_pairs)
    CHECK.assertEqual(["BASE-USD", "ETH-USD"], tracker._trading_pairs)
    CHECK.assertEqual({"BASE-USD", "ETH-USD"}, set(tracker.order_books))
    CHECK.assertIs(eth_book, tracker.order_books["ETH-USD"])
    CHECK.assertEqual(0, connector.stop_calls)


@pytest.mark.asyncio
async def test_issue224_start_rollback_uses_value_count_after_unrelated_insertion():
    _, service, connector, _ = _issue224_repair_service()
    service._data_connectors_started = {}
    start_entered = asyncio.Event()
    release_start = asyncio.Event()

    async def start_network():
        start_entered.set()
        await release_start.wait()
        return False

    connector.start_network = start_network
    request = asyncio.create_task(
        service.ensure_data_connector_started("manzana", "BTC-USD")
    )
    await start_entered.wait()
    connector._trading_pairs.insert(0, "ETH-USD")
    release_start.set()

    CHECK.assertIs(await request, False)
    CHECK.assertIs(connector, service._data_connectors["manzana"])
    CHECK.assertEqual(["ETH-USD", "BASE-USD"], connector._trading_pairs)
    CHECK.assertEqual(0, connector.stop_calls)


@pytest.mark.asyncio
async def test_issue224_ambiguous_pair_duplicates_force_discard():
    _, service, connector, _ = _issue224_repair_service()
    connector._trading_pairs = ["BASE-USD", "BTC-USD", "BTC-USD"]

    async def remove_one_duplicate(connector_arg, trading_pair):
        connector_arg._trading_pairs.remove(trading_pair)
        return False

    service._add_trading_pair_to_tracker = remove_one_duplicate

    CHECK.assertIs(
        await service.initialize_order_book("manzana", "BTC-USD", timeout=0),
        False,
    )
    CHECK.assertEqual({}, service._data_connectors)
    CHECK.assertEqual({}, service._data_connectors_started)
    CHECK.assertEqual(1, connector.stop_calls)
    CHECK.assertEqual(1, connector._trading_pairs.count("BTC-USD"))


@pytest.mark.asyncio
async def test_issue224_unproven_task_set_mutation_discards_without_claiming_member():
    _, service, connector, tracker = _issue224_repair_service()
    pair_entered = asyncio.Event()
    release_pair = asyncio.Event()

    async def pending():
        await asyncio.Event().wait()

    unrelated_task = asyncio.create_task(pending())

    async def add_pair(connector_arg, trading_pair):
        connector_arg._trading_pairs.append(trading_pair)
        tracker._trading_pairs.append(trading_pair)
        pair_entered.set()
        await release_pair.wait()
        return False

    service._add_trading_pair_to_tracker = add_pair
    request = asyncio.create_task(
        service.initialize_order_book("manzana", "BTC-USD", timeout=0)
    )
    await pair_entered.wait()
    tracker._in_flight_transactions.add(unrelated_task)
    release_pair.set()

    try:
        CHECK.assertIs(await request, False)
        CHECK.assertEqual({}, service._data_connectors)
        CHECK.assertEqual({}, service._data_connectors_started)
        CHECK.assertEqual(1, connector.stop_calls)
        CHECK.assertFalse(unrelated_task.done())
        CHECK.assertIn(unrelated_task, tracker._in_flight_transactions)
    finally:
        unrelated_task.cancel()
        await asyncio.gather(unrelated_task, return_exceptions=True)


@pytest.mark.asyncio
async def test_issue224_owned_pair_and_book_removals_are_restored_in_place():
    _, service, connector, tracker = _issue224_repair_service()
    baseline_book = object()
    connector._trading_pairs.append("BTC-USD")
    tracker._trading_pairs.append("BTC-USD")
    tracker.order_books["BTC-USD"] = baseline_book

    async def remove_pair(connector_arg, trading_pair):
        connector_arg._trading_pairs.remove(trading_pair)
        tracker._trading_pairs.remove(trading_pair)
        tracker.order_books.pop(trading_pair)
        return False

    service._add_trading_pair_to_tracker = remove_pair

    CHECK.assertIs(
        await service.initialize_order_book("manzana", "BTC-USD", timeout=0),
        False,
    )
    CHECK.assertIs(connector, service._data_connectors["manzana"])
    CHECK.assertIn("BTC-USD", connector._trading_pairs)
    CHECK.assertIn("BTC-USD", tracker._trading_pairs)
    CHECK.assertIs(baseline_book, tracker.order_books["BTC-USD"])
    CHECK.assertEqual(0, connector.stop_calls)


@pytest.mark.asyncio
async def test_issue224_uninstrumented_book_wait_mutation_is_unpublished_not_rewound():
    _, service, connector, tracker = _issue224_repair_service()
    hidden_task = None

    async def hidden_book_wait(*args):
        nonlocal hidden_task
        connector._trading_pairs.append("HIDDEN-USD")
        tracker._trading_pairs.append("HIDDEN-USD")
        tracker.order_books["HIDDEN-USD"] = Issue224RepairBook()
        hidden_task = asyncio.create_task(asyncio.Event().wait())
        tracker._tracking_tasks["HIDDEN-USD"] = hidden_task
        await asyncio.sleep(0)
        return False

    service._add_trading_pair_to_tracker = lambda *args: asyncio.sleep(
        0, result=True
    )
    service._wait_for_order_book = hidden_book_wait

    CHECK.assertIs(
        await service.initialize_order_book("manzana", "BTC-USD", timeout=0),
        False,
    )
    CHECK.assertEqual({}, service._data_connectors)
    CHECK.assertEqual({}, service._data_connectors_started)
    CHECK.assertEqual(1, connector.stop_calls)
    CHECK.assertIn("HIDDEN-USD", connector._trading_pairs)
    CHECK.assertIn("HIDDEN-USD", tracker._trading_pairs)
    CHECK.assertIn("HIDDEN-USD", tracker.order_books)
    CHECK.assertIs(hidden_task, tracker._tracking_tasks["HIDDEN-USD"])
    CHECK.assertFalse(hidden_task.done())
    hidden_task.cancel()
    await asyncio.gather(hidden_task, return_exceptions=True)


@pytest.mark.asyncio
async def test_issue224_task_ledger_cancels_owned_handle_not_replacement():
    _, service, connector, tracker = _issue224_repair_service()
    owned_published = asyncio.Event()
    release_pair = asyncio.Event()

    async def pending():
        await asyncio.Event().wait()

    owned_task = None
    set_task = None

    async def add_pair(connector_arg, trading_pair):
        nonlocal owned_task, set_task
        owned_task = asyncio.create_task(pending())
        set_task = asyncio.create_task(pending())
        tracker._tracking_tasks[trading_pair] = owned_task
        tracker._in_flight_transactions.add(set_task)
        owned_published.set()
        await release_pair.wait()
        return False

    service._add_trading_pair_to_tracker = add_pair
    request = asyncio.create_task(
        service.initialize_order_book("manzana", "BTC-USD", timeout=0)
    )
    await owned_published.wait()
    replacement_task = asyncio.create_task(pending())
    tracker._tracking_tasks["BTC-USD"] = replacement_task
    release_pair.set()

    try:
        CHECK.assertIs(await request, False)
        CHECK.assertTrue(owned_task.done())
        CHECK.assertTrue(owned_task.cancelled())
        CHECK.assertTrue(set_task.done())
        CHECK.assertTrue(set_task.cancelled())
        CHECK.assertFalse(replacement_task.done())
        CHECK.assertIs(replacement_task, tracker._tracking_tasks["BTC-USD"])
        CHECK.assertEqual({}, service._data_connectors)
        CHECK.assertEqual({}, service._data_connectors_started)
        CHECK.assertEqual(1, connector.stop_calls)
    finally:
        for task in (owned_task, set_task, replacement_task):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(
            *(
                task
                for task in (owned_task, set_task, replacement_task)
                if task is not None
            ),
            return_exceptions=True,
        )


@pytest.mark.asyncio
async def test_issue224_task_ledger_retains_all_explicit_owned_generations():
    _, service, connector, tracker = _issue224_repair_service()
    owned_tasks = []

    async def pending():
        await asyncio.Event().wait()

    async def add_pair(connector_arg, trading_pair):
        first = asyncio.create_task(pending())
        owned_tasks.append(first)
        tracker._tracking_tasks[trading_pair] = first
        await asyncio.sleep(0)
        second = asyncio.create_task(pending())
        owned_tasks.append(second)
        tracker._tracking_tasks[trading_pair] = second
        return False

    service._add_trading_pair_to_tracker = add_pair

    CHECK.assertIs(
        await service.initialize_order_book("manzana", "BTC-USD", timeout=0),
        False,
    )
    CHECK.assertEqual(2, len(owned_tasks))
    CHECK.assertTrue(all(task.done() for task in owned_tasks))
    CHECK.assertTrue(all(task.cancelled() for task in owned_tasks))
    CHECK.assertNotIn("BTC-USD", tracker._tracking_tasks)
    CHECK.assertIs(connector, service._data_connectors["manzana"])
    CHECK.assertEqual(0, connector.stop_calls)


@pytest.mark.asyncio
async def test_issue224_trading_unsafe_discard_commits_owner_with_one_stop():
    _, service, connector, _ = _issue224_repair_service()
    service._data_connectors = {}
    service._data_connectors_started = {}
    service._trading_connectors = {"acct": {"manzana": connector}}

    async def replace_pairs(connector_arg, trading_pair):
        connector_arg._trading_pairs = ["POISON-USD"]
        return False

    service._add_trading_pair_to_tracker = replace_pairs

    CHECK.assertIs(
        await service.initialize_order_book(
            "manzana", "BTC-USD", account_name="acct", timeout=0
        ),
        False,
    )
    owner_key = ("acct", "manzana", "public_transaction")
    CHECK.assertEqual(1, connector.stop_calls)
    CHECK.assertEqual({}, service._trading_connectors)
    CHECK.assertNotIn(owner_key, service._connector_cleanup_owners)
    CHECK.assertNotIn(owner_key, service._connector_residual_tasks)
    CHECK.assertNotIn(owner_key, service._connector_cleanup_phases)
    CHECK.assertEqual({}, service._connector_creation_residuals)

    await service._retry_public_transaction_cleanup(("acct", "manzana"))
    CHECK.assertEqual(1, connector.stop_calls)


@pytest.mark.asyncio
async def test_issue224_trading_discard_preserves_other_private_residuals():
    module, service, connector, _ = _issue224_repair_service()
    service._data_connectors = {}
    service._data_connectors_started = {}
    service._trading_connectors = {"acct": {"manzana": connector}}
    other_owner_key = ("acct", "manzana", "other_private")
    other_owner = object()
    service._connector_cleanup_owners[other_owner_key] = other_owner

    async def replace_pairs(connector_arg, trading_pair):
        connector_arg._trading_pairs = ["POISON-USD"]
        return False

    service._add_trading_pair_to_tracker = replace_pairs

    with pytest.raises(module.ConnectorCapabilityError) as blocked:
        await service.initialize_order_book(
            "manzana", "BTC-USD", account_name="acct", timeout=0
        )

    public_owner_key = ("acct", "manzana", "public_transaction")
    CHECK.assertEqual("connector_cleanup_residual", blocked.value.detail["code"])
    CHECK.assertEqual("owner_cleanup_incomplete", blocked.value.detail["cleanup_code"])
    CHECK.assertEqual(1, connector.stop_calls)
    CHECK.assertIs(other_owner, service._connector_cleanup_owners[other_owner_key])
    CHECK.assertNotIn(public_owner_key, service._connector_cleanup_owners)
    CHECK.assertNotIn(public_owner_key, service._connector_cleanup_phases)

    await service._retry_public_transaction_cleanup(("acct", "manzana"))
    CHECK.assertEqual(1, connector.stop_calls)


@pytest.mark.asyncio
@pytest.mark.parametrize("restart_stop_outcome", ["false", "raise", "pending"])
async def test_issue224_data_discard_reconciles_restart_before_one_successor(
    restart_stop_outcome,
):
    module, service, old_connector, tracker = _issue224_repair_service()
    release_stop = asyncio.Event()
    stop_cancelled = asyncio.Event()
    tracker.stop_calls = 0

    if restart_stop_outcome == "pending":

        async def stop():
            tracker.stop_calls += 1
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                stop_cancelled.set()
                await release_stop.wait()

    else:

        def stop():
            tracker.stop_calls += 1
            if restart_stop_outcome == "raise":
                raise RuntimeError("PRIVATE-RESTART-STOP-SECRET")
            return False

    tracker.stop = stop
    restart = await service.restart_order_book_tracker("manzana")
    restart_owner = ("__data__", "manzana", "restart")
    public_owner = ("__data__", "manzana", "public_transaction")
    connector_key = ("__data__", "manzana")
    expected_error = (
        "tracker_cleanup_pending"
        if restart_stop_outcome == "pending"
        else "tracker_stop_failed"
    )
    CHECK.assertEqual(expected_error, restart["error"])
    CHECK.assertIn(restart_owner, service._connector_cleanup_owners)
    CHECK.assertEqual(1, tracker.stop_calls)

    successor = Issue224RepairConnector()
    factory_calls = 0

    def create_successor(connector_name):
        nonlocal factory_calls
        factory_calls += 1
        service._data_connectors_started[connector_name] = True
        return successor

    async def replace_old_pairs_or_publish_successor(connector, trading_pair):
        if connector is old_connector:
            connector._trading_pairs = ["POISON-USD"]
            return False
        connector._trading_pairs.append(trading_pair)
        connector.order_book_tracker._trading_pairs.append(trading_pair)
        connector.order_book_tracker.order_books[trading_pair] = Issue224RepairBook()
        return True

    service._create_data_connector = create_successor
    service._add_trading_pair_to_tracker = replace_old_pairs_or_publish_successor
    service._wait_for_order_book = lambda *args: asyncio.sleep(0, result=True)

    if restart_stop_outcome == "pending":
        with pytest.raises(module.ConnectorCapabilityError) as first_blocked:
            await service.initialize_order_book("manzana", "BTC-USD", timeout=0)
        await stop_cancelled.wait()
        CHECK.assertEqual(
            "connector_cleanup_residual", first_blocked.value.detail["code"]
        )
        CHECK.assertEqual(
            "restart_cleanup_incomplete",
            first_blocked.value.detail["cleanup_code"],
        )
        CHECK.assertIs(old_connector, service._data_connectors["manzana"])
        CHECK.assertEqual(0, old_connector.stop_calls)
        CHECK.assertIn(restart_owner, service._connector_cleanup_owners)
        CHECK.assertNotIn(public_owner, service._connector_cleanup_owners)
        CHECK.assertNotIn(connector_key, service._connector_creation_residuals)

        with pytest.raises(module.ConnectorCapabilityError):
            await service.initialize_order_book("manzana", "BTC-USD", timeout=0)
        CHECK.assertEqual(0, factory_calls)
        CHECK.assertEqual(1, tracker.stop_calls)

        restart_stop_task = service._connector_cleanup_phases[restart_owner][
            "restart_stop"
        ]["task"]
        release_stop.set()
        await restart_stop_task
        for _ in range(100):
            if not service._market_locks:
                break
            await asyncio.sleep(0)
    else:
        CHECK.assertIs(
            await service.initialize_order_book("manzana", "BTC-USD", timeout=0),
            True,
        )
        CHECK.assertEqual(1, old_connector.stop_calls)
        CHECK.assertNotIn(restart_owner, service._connector_cleanup_owners)
        CHECK.assertNotIn(public_owner, service._connector_cleanup_owners)
        CHECK.assertNotIn(connector_key, service._connector_creation_residuals)
        CHECK.assertEqual(1, tracker.stop_calls)
        CHECK.assertEqual(1, factory_calls)

    CHECK.assertIs(
        await service.initialize_order_book("manzana", "BTC-USD", timeout=0),
        True,
    )
    CHECK.assertIs(successor, service._data_connectors["manzana"])
    CHECK.assertEqual(1, old_connector.stop_calls)
    CHECK.assertEqual(1, tracker.stop_calls)
    CHECK.assertEqual(1, factory_calls)
    CHECK.assertEqual({}, service._connector_creation_residuals)
    CHECK.assertEqual({}, service._connector_cleanup_owners)
    CHECK.assertEqual({}, service._connector_residual_tasks)
    CHECK.assertEqual({}, service._connector_cleanup_phases)

    CHECK.assertIs(
        await service.initialize_order_book("manzana", "BTC-USD", timeout=0),
        True,
    )
    CHECK.assertEqual(1, factory_calls)


@pytest.mark.asyncio
@pytest.mark.parametrize("connector_type", ["data", "trading"])
@pytest.mark.parametrize("private_state", ["owner", "residual"])
async def test_issue224_orphan_private_state_recovers_before_one_successor(
    caplog, connector_type, private_state
):
    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)
    old_connector = Issue224RepairConnector()
    successor = Issue224RepairConnector()
    account_name = "__data__" if connector_type == "data" else "acct"
    owner_kind = "data_network" if connector_type == "data" else "network"
    connector_key = (account_name, "manzana")
    owner_key = (*connector_key, owner_kind)

    if private_state == "owner":
        service._connector_cleanup_owners[owner_key] = old_connector
    else:
        service._connector_creation_residuals[connector_key] = {
            "code": "connector_cleanup_residual",
            "message": "Connector cleanup is incomplete",
            "connector": "manzana",
            "feature": "connector_cleanup",
            "reason": "cleanup_incomplete",
            "cleanup_scope": "connector",
            "cleanup_code": "owner_cleanup_incomplete",
        }

    factory_calls = 0

    def create_successor(connector_name):
        nonlocal factory_calls
        factory_calls += 1
        return successor

    async def create_trading_successor(account, connector_name):
        CHECK.assertEqual(account_name, account)
        return create_successor(connector_name)

    async def get_connector():
        if connector_type == "data":
            return service.get_data_connector("manzana")
        return await service.get_trading_connector(account_name, "manzana")

    if connector_type == "data":
        service._create_data_connector = create_successor
    else:
        service._create_and_initialize_trading_connector = create_trading_successor
    with pytest.raises(module.ConnectorCapabilityError) as blocked:
        await get_connector()

    CHECK.assertEqual("connector_cleanup_residual", blocked.value.detail["code"])
    CHECK.assertEqual(0, factory_calls)
    CHECK.assertIn(connector_key, service._connector_creation_residuals)

    caplog.set_level("INFO")
    await service.stop_all()
    await service.stop_all()

    CHECK.assertEqual(1 if private_state == "owner" else 0, old_connector.stop_calls)
    CHECK.assertNotIn("orphan_terminal_phase_consumed", caplog.text)
    CHECK.assertEqual({}, service._connector_creation_residuals)
    CHECK.assertEqual({}, service._connector_cleanup_owners)
    CHECK.assertEqual({}, service._connector_cleanup_indices)
    CHECK.assertEqual({}, service._connector_residual_tasks)
    CHECK.assertEqual({}, service._connector_cleanup_phases)

    CHECK.assertIs(successor, await get_connector())
    CHECK.assertIs(successor, await get_connector())
    CHECK.assertEqual(1, factory_calls)
    CHECK.assertEqual({}, service._connector_creation_residuals)
    CHECK.assertEqual({}, service._connector_cleanup_owners)
    CHECK.assertEqual({}, service._connector_residual_tasks)
    CHECK.assertEqual({}, service._connector_cleanup_phases)

    await service.stop_all()
    await service.stop_all()
    CHECK.assertEqual(1, successor.stop_calls)
    CHECK.assertEqual({}, service._trading_connectors)
    CHECK.assertEqual({}, service._data_connectors)
    CHECK.assertEqual({}, service._connector_creation_residuals)
    CHECK.assertEqual({}, service._connector_cleanup_owners)
    CHECK.assertEqual({}, service._connector_cleanup_indices)
    CHECK.assertEqual({}, service._connector_residual_tasks)
    CHECK.assertEqual({}, service._connector_cleanup_phases)
    CHECK.assertEqual({}, service._connector_incarnations)


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal_result", [True, False])
async def test_issue224_orphan_phase_inspects_terminal_task_result(terminal_result):
    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)
    service._ensure_connector_cleanup_state()
    connector_key = ("acct", "manzana")
    owner_key = (*connector_key, "network")
    task = asyncio.create_task(asyncio.sleep(0, result=terminal_result))
    await task
    service._connector_cleanup_phases[owner_key] = {
        "connector_stop": {
            "task": task,
            "complete": False,
            "result": None,
            "error_code": None,
            "cancel_requested": False,
        }
    }
    service._connector_residual_tasks[owner_key] = {task}

    if terminal_result:
        await service.stop_all()
        CHECK.assertEqual({}, service._connector_creation_residuals)
        CHECK.assertEqual({}, service._connector_residual_tasks)
        CHECK.assertEqual({}, service._connector_cleanup_phases)
    else:
        with pytest.raises(module.ConnectorCapabilityError) as incomplete:
            await service.stop_all()
        CHECK.assertEqual(
            "network_cleanup_incomplete", incomplete.value.detail["cleanup_code"]
        )
        CHECK.assertEqual({}, service._connector_residual_tasks)
        CHECK.assertIn(owner_key, service._connector_cleanup_phases)
        phase = service._connector_cleanup_phases[owner_key]["connector_stop"]
        CHECK.assertIs(phase["complete"], True)
        CHECK.assertIs(phase["result"], False)
        CHECK.assertEqual("connector_stop_returned_false", phase["error_code"])


@pytest.mark.asyncio
async def test_issue224_orphan_phase_retains_live_operation():
    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)
    owner_key = ("acct", "manzana", "network")
    release = asyncio.Event()
    task = asyncio.create_task(release.wait())
    service._connector_cleanup_phases[owner_key] = {
        "connector_stop": {
            "task": task,
            "complete": False,
            "result": None,
            "error_code": None,
            "cancel_requested": False,
        }
    }

    try:
        with pytest.raises(module.ConnectorCapabilityError) as incomplete:
            await service.stop_all()
        CHECK.assertEqual(
            "network_cleanup_incomplete", incomplete.value.detail["cleanup_code"]
        )
        phase = service._connector_cleanup_phases[owner_key]["connector_stop"]
        CHECK.assertIs(phase["task"], task)
        CHECK.assertFalse(task.done())
    finally:
        release.set()
        await task


@pytest.mark.asyncio
async def test_issue224_invalid_data_declaration_reconciles_restart_owner():
    module, service, connector, tracker = _issue224_repair_service()
    tracker.stop_calls = 0

    def stop():
        tracker.stop_calls += 1
        return False

    tracker.stop = stop
    restart = await service.restart_order_book_tracker("manzana")
    restart_owner = ("__data__", "manzana", "restart")
    CHECK.assertEqual("tracker_stop_failed", restart["error"])
    CHECK.assertIn(restart_owner, service._connector_cleanup_owners)

    declaration = _shift_successor_capabilities()
    declaration["features"] = set(declaration["features"]) - {
        module.ConnectorFeature.LIVE_ORDER_BOOK
    }
    connector.api_capabilities = declaration

    with pytest.raises(module.ConnectorCapabilityError) as mismatch:
        await service.initialize_order_book("manzana", "BTC-USD", timeout=0)

    CHECK.assertEqual("connector_capability_mismatch", mismatch.value.detail["code"])
    CHECK.assertEqual(1, tracker.stop_calls)
    CHECK.assertEqual(1, connector.stop_calls)
    CHECK.assertEqual({}, service._data_connectors)
    CHECK.assertEqual({}, service._data_connectors_started)
    CHECK.assertNotIn(restart_owner, service._connector_cleanup_owners)
    CHECK.assertEqual({}, service._connector_creation_residuals)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_path", ["add", "fallback"])
async def test_issue224_dynamic_subscription_logs_stable_code_without_secret(
    caplog, failure_path
):
    _, service, connector, tracker = _issue224_repair_service()
    secret = "PRIVATE-DYNAMIC-SUBSCRIPTION-SECRET"
    keep_running = asyncio.Event()
    listener = asyncio.create_task(keep_running.wait())
    tracker._order_book_stream_listener_task = listener
    tracker._init_order_books_task = None
    tracker.ready = True

    async def add_trading_pair(trading_pair):
        if failure_path == "add":
            raise RuntimeError(secret)
        return False

    class DataSource:
        async def get_new_order_book(self, trading_pair):
            raise RuntimeError(secret)

    connector.add_trading_pair = add_trading_pair
    connector._orderbook_ds = DataSource()
    caplog.set_level("ERROR")

    try:
        CHECK.assertIs(
            await service._add_trading_pair_to_tracker(connector, "BTC-USD"),
            False,
        )
        records = [
            record
            for record in caplog.records
            if "dynamic_subscription" in record.getMessage()
        ]
        CHECK.assertEqual(1, len(records))
        CHECK.assertIn("RuntimeError", records[0].getMessage())
        CHECK.assertNotIn(secret, records[0].getMessage())
        CHECK.assertIsNone(records[0].exc_info)
    finally:
        listener.cancel()
        await asyncio.gather(listener, return_exceptions=True)


@pytest.mark.asyncio
async def test_issue224_revoked_task_token_cannot_retain_or_mutate_old_snapshot():
    import gc
    import weakref

    module, service, connector, _ = _issue224_repair_service()
    first_snapshot = service._snapshot_public_connector_state(
        "manzana",
        connector,
        ("__data__", "manzana"),
        service._capability_registry.get("manzana"),
        trading_pair="BTC-USD",
    )

    class RetentionProbe:
        pass

    probe = RetentionProbe()
    probe_ref = weakref.ref(probe)
    first_snapshot["retention_probe"] = probe
    release_child = asyncio.Event()
    inherited_token = asyncio.get_running_loop().create_future()
    nested_task_created = asyncio.get_running_loop().create_future()

    async def long_lived_child():
        inherited_token.set_result(module._PUBLIC_TRANSACTION_CONTEXT.get())
        await release_child.wait()
        nested = asyncio.create_task(asyncio.sleep(0))
        nested_task_created.set_result(nested)
        await nested

    async with service._capture_public_task_provenance(first_snapshot):
        child = asyncio.create_task(long_lived_child())
        token = await inherited_token

    first_provenance = first_snapshot["provenance_tasks"]
    first_owned = first_snapshot["owned_tasks"]
    CHECK.assertIsNot(first_snapshot, token)
    CHECK.assertTrue(hasattr(token, "id"))
    CHECK.assertTrue(hasattr(token, "active"))
    CHECK.assertEqual({"id", "active"}, set(type(token).__slots__))
    CHECK.assertIs(token.active, False)
    CHECK.assertEqual({}, service._active_public_transaction_tokens)
    CHECK.assertIsNone(module._PUBLIC_TRANSACTION_CONTEXT.get())
    CHECK.assertEqual({child}, first_provenance)
    CHECK.assertEqual({child}, first_owned)

    del first_snapshot
    del probe
    gc.collect()
    CHECK.assertIsNone(probe_ref())

    second_snapshot = service._snapshot_public_connector_state(
        "manzana",
        connector,
        ("__data__", "manzana"),
        service._capability_registry.get("manzana"),
        trading_pair="ETH-USD",
    )
    async with service._capture_public_task_provenance(second_snapshot):
        release_child.set()
        nested = await nested_task_created
        await nested
    await child

    CHECK.assertEqual({child}, first_provenance)
    CHECK.assertEqual({child}, first_owned)
    CHECK.assertNotIn(nested, second_snapshot["provenance_tasks"])
    CHECK.assertEqual({}, service._active_public_transaction_tokens)


@pytest.mark.asyncio
async def test_issue224_unsafe_discard_survives_resistant_task_retry():
    module, service, connector, tracker = _issue224_repair_service()
    cancellation_seen = asyncio.Event()
    release_owned = asyncio.Event()
    owned_task = None

    async def resistant():
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancellation_seen.set()
            await release_owned.wait()

    async def add_pair(connector_arg, trading_pair):
        nonlocal owned_task
        connector_arg._trading_pairs = ["POISON-USD"]
        owned_task = asyncio.create_task(resistant())
        tracker._tracking_tasks[trading_pair] = owned_task
        await asyncio.sleep(0)
        return False

    service._add_trading_pair_to_tracker = add_pair
    with pytest.raises(module.ConnectorCapabilityError) as pending:
        await service.initialize_order_book("manzana", "BTC-USD", timeout=0)

    await cancellation_seen.wait()
    CHECK.assertEqual(
        "connector_capability_cleanup_residual", pending.value.detail["code"]
    )
    CHECK.assertIs(connector, service._data_connectors["manzana"])
    CHECK.assertEqual(0, connector.stop_calls)

    release_owned.set()
    await owned_task
    await service._retry_public_transaction_cleanup(("__data__", "manzana"))

    CHECK.assertEqual({}, service._data_connectors)
    CHECK.assertEqual({}, service._data_connectors_started)
    CHECK.assertEqual(1, connector.stop_calls)
    CHECK.assertEqual({}, service._connector_creation_residuals)
    CHECK.assertEqual({}, service._connector_cleanup_owners)
    CHECK.assertEqual({}, service._connector_residual_tasks)
    CHECK.assertEqual({}, service._connector_cleanup_phases)


@pytest.mark.asyncio
async def test_issue224_remove_pair_uses_initialization_owner_lock():
    module, service, connector, tracker = _issue224_repair_service()
    tracker.order_books["BTC-USD"] = types.SimpleNamespace(snapshot=([], []))
    connector._trading_pairs.append("BTC-USD")
    tracker._trading_pairs.append("BTC-USD")
    pair_entered = asyncio.Event()
    release_pair = asyncio.Event()

    async def add_pair(connector_arg, trading_pair):
        pair_entered.set()
        await release_pair.wait()
        return False

    async def remove_pair(trading_pair):
        connector._trading_pairs.remove(trading_pair)
        tracker._trading_pairs.remove(trading_pair)
        tracker.order_books.pop(trading_pair, None)
        return True

    service._add_trading_pair_to_tracker = add_pair
    connector.remove_trading_pair = remove_pair
    initialization = asyncio.create_task(
        service.initialize_order_book("manzana", "BTC-USD", timeout=0)
    )
    await pair_entered.wait()
    removal = asyncio.create_task(
        service.remove_trading_pair("manzana", "BTC-USD")
    )

    try:
        with pytest.raises(module.ConnectorCapabilityError) as active_root:
            await removal
        CHECK.assertEqual(
            "market_transaction_retained",
            active_root.value.detail["cleanup_code"],
        )
        release_pair.set()
        CHECK.assertIs(await initialization, False)
        CHECK.assertIs(
            await service.remove_trading_pair("manzana", "BTC-USD"),
            True,
        )
        CHECK.assertNotIn("BTC-USD", connector._trading_pairs)
        CHECK.assertNotIn("BTC-USD", tracker._trading_pairs)
        CHECK.assertNotIn("BTC-USD", tracker.order_books)
    finally:
        release_pair.set()
        await asyncio.gather(initialization, removal, return_exceptions=True)


@pytest.mark.asyncio
async def test_issue224_task_reconciliation_does_not_busy_spin():
    _, service, _, _ = _issue224_repair_service()
    pair_entered = asyncio.Event()
    release_pair = asyncio.Event()
    scans = 0
    discover = service._discover_public_transaction_tasks

    def counted_discovery(snapshot, boundary):
        nonlocal scans
        scans += 1
        return discover(snapshot, boundary)

    async def add_pair(connector_arg, trading_pair):
        pair_entered.set()
        await release_pair.wait()
        return False

    service._discover_public_transaction_tasks = counted_discovery
    service._add_trading_pair_to_tracker = add_pair
    request = asyncio.create_task(
        service.initialize_order_book("manzana", "BTC-USD", timeout=0)
    )
    await pair_entered.wait()
    await asyncio.sleep(0.01)

    try:
        CHECK.assertLessEqual(scans, 2)
    finally:
        release_pair.set()
        await request


@pytest.mark.asyncio
async def test_issue224_cancellation_resistant_owned_task_is_retained_and_retried():
    module, service, connector, tracker = _issue224_repair_service()
    release_owned = asyncio.Event()
    cancellation_seen = asyncio.Event()
    owned_task = None

    async def resistant():
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancellation_seen.set()
            await release_owned.wait()

    async def add_pair(connector_arg, trading_pair):
        nonlocal owned_task
        owned_task = asyncio.create_task(resistant())
        tracker._tracking_tasks[trading_pair] = owned_task
        await asyncio.sleep(0)
        return False

    service._add_trading_pair_to_tracker = add_pair
    with pytest.raises(module.ConnectorCapabilityError) as pending:
        await service.initialize_order_book("manzana", "BTC-USD", timeout=0)

    await cancellation_seen.wait()
    owner_key = ("__data__", "manzana", "public_transaction")
    CHECK.assertEqual(
        "connector_capability_cleanup_residual", pending.value.detail["code"]
    )
    CHECK.assertIs(connector, service._data_connectors["manzana"])
    CHECK.assertIn(owner_key, service._connector_cleanup_owners)
    CHECK.assertIn(owned_task, service._connector_residual_tasks[owner_key])
    CHECK.assertIn(owner_key, service._connector_cleanup_phases)
    CHECK.assertFalse(owned_task.done())

    with pytest.raises(module.ConnectorCapabilityError) as retry_pending:
        await service.initialize_order_book("manzana", "BTC-USD", timeout=0)
    CHECK.assertEqual(
        "connector_capability_cleanup_residual",
        retry_pending.value.detail["code"],
    )
    CHECK.assertIn(owned_task, service._connector_residual_tasks[owner_key])
    CHECK.assertFalse(owned_task.done())

    release_owned.set()
    await owned_task

    async def successful_retry(connector_arg, trading_pair):
        connector_arg._trading_pairs.append(trading_pair)
        tracker._trading_pairs.append(trading_pair)
        tracker.order_books[trading_pair] = Issue224RepairBook()
        return True

    service._add_trading_pair_to_tracker = successful_retry
    service._wait_for_order_book = lambda *args: asyncio.sleep(0, result=True)

    CHECK.assertIs(
        await service.initialize_order_book("manzana", "BTC-USD", timeout=0),
        True,
    )
    CHECK.assertEqual({}, service._connector_creation_residuals)
    CHECK.assertEqual({}, service._connector_cleanup_owners)
    CHECK.assertEqual({}, service._connector_residual_tasks)
    CHECK.assertEqual({}, service._connector_cleanup_phases)


@pytest.mark.asyncio
async def test_issue224_pair_raise_is_typed_and_sanitized_through_public_callers():
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from models.connectors import CapabilityErrorDetail, CapabilityErrorResponse

    deps = sys.modules.get("deps")
    if deps is not None:
        deps.get_executor_service = lambda: None
        deps.get_market_data_service = lambda: None
    executor_service_module = _module("services.executor_service")
    executor_service_module.ExecutorService = type("ExecutorService", (), {})
    from routers import executors as executors_router

    module, service, _, _ = _issue224_repair_service()
    secret = "PRIVATE-PAIR-SECRET"

    async def raise_pair(connector, trading_pair):
        raise RuntimeError(secret)

    service._add_trading_pair_to_tracker = raise_pair
    with pytest.raises(module.ConnectorCapabilityError) as failure:
        await service.initialize_order_book("manzana", "BTC-USD", timeout=0)

    CHECK.assertEqual(
        "connector_pair_registration_failed", failure.value.detail["code"]
    )
    CHECK.assertNotIn(secret, json.dumps(failure.value.detail))

    class ExecutorService:
        async def create_executor(self, **kwargs):
            try:
                raise RuntimeError(secret)
            except RuntimeError as cause:
                raise module.ConnectorCapabilityError(
                    CapabilityErrorDetail.model_validate(failure.value.detail)
                ) from cause

    app = FastAPI()
    app.include_router(executors_router.router)
    app.dependency_overrides[executors_router.get_executor_service] = ExecutorService
    response = TestClient(app).post(
        "/executors/",
        json={"executor_config": {"type": "position_executor"}},
    )
    CHECK.assertEqual(400, response.status_code, response.text)
    CHECK.assertEqual(
        "connector_pair_registration_failed", response.json()["detail"]["code"]
    )
    CHECK.assertNotIn(secret, response.text)
    CapabilityErrorResponse.model_validate(response.json())
    responses = app.openapi()["paths"]["/executors/"]["post"]["responses"]
    CHECK.assertEqual(
        "#/components/schemas/CapabilityErrorResponse",
        responses["400"]["content"]["application/json"]["schema"]["$ref"],
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["initialize", "ensure_started"])
async def test_issue224_invalid_preloaded_truth_is_cleaned_before_early_success(operation):
    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)
    declaration = _shift_successor_capabilities()
    declaration["features"] = set(declaration["features"]) - {
        module.ConnectorFeature.LIVE_ORDER_BOOK
    }

    class Connector:
        order_book_tracker = None

        def __init__(self):
            self.api_capabilities = declaration
            self._trading_pairs = []
            self.stop_calls = 0

        async def stop_network(self):
            self.stop_calls += 1

    connector = Connector()
    service._data_connectors = {"manzana": connector}
    service._data_connectors_started = {"manzana": True}

    with pytest.raises(module.ConnectorCapabilityError) as mismatch:
        if operation == "initialize":
            await service.initialize_order_book("manzana", "BTC-USD", timeout=0)
        else:
            await service.ensure_data_connector_started("manzana", "BTC-USD")

    CHECK.assertEqual("connector_capability_mismatch", mismatch.value.detail["code"])
    CHECK.assertEqual(1, connector.stop_calls)
    CHECK.assertEqual({}, service._data_connectors)
    CHECK.assertEqual({}, service._data_connectors_started)


@pytest.mark.parametrize("outcome", ["false", "raise"])
def test_issue224_add_pair_failure_has_one_typed_500_contract(outcome):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from models.connectors import CapabilityErrorDetail, PublicErrorResponse
    from routers import market_data as market_data_router
    from services.connector_capabilities import ConnectorCapabilityError

    secret = "PRIVATE-PAIR-HTTP-SECRET"

    class Service:
        def require_capability(self, *args, **kwargs):
            return None

        async def initialize_order_book(self, **kwargs):
            if outcome == "raise":
                raise ConnectorCapabilityError(
                    CapabilityErrorDetail(
                        code="connector_pair_registration_failed",
                        message="Connector pair registration failed",
                        connector=kwargs["connector_name"],
                        feature="live_order_book",
                        trading_pair=kwargs["trading_pair"],
                        reason="pair_registration_failed",
                    )
                ) from RuntimeError(secret)
            return False

    app = FastAPI()
    app.include_router(market_data_router.router)
    app.dependency_overrides[market_data_router.get_market_data_service] = Service
    response = TestClient(app).post(
        "/market-data/trading-pair/add",
        json={
            "connector_name": "manzana",
            "trading_pair": "BTC-USD",
            "timeout": 1,
        },
    )

    CHECK.assertEqual(500, response.status_code, response.text)
    PublicErrorResponse.model_validate(response.json())
    CHECK.assertEqual(
        {
            "detail": {
                "code": "connector_pair_registration_failed",
                "message": "Connector pair registration failed",
            }
        },
        response.json(),
    )
    CHECK.assertNotIn(secret, response.text)
    responses = app.openapi()["paths"]["/market-data/trading-pair/add"]["post"][
        "responses"
    ]
    CHECK.assertEqual(
        "#/components/schemas/PublicErrorResponse",
        responses["500"]["content"]["application/json"]["schema"]["$ref"],
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("account_name", "owner_kind"),
    [("acct", "network"), ("__data__", "data_network")],
)
async def test_issue224_empty_phase_cannot_consume_same_key_residual(
    account_name, owner_kind
):
    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)
    connector_key = (account_name, "manzana")
    owner_key = (*connector_key, owner_kind)
    residual = {
        "code": "connector_cleanup_residual",
        "message": "Connector cleanup is incomplete",
        "connector": "manzana",
        "feature": "connector_cleanup",
        "reason": "cleanup_incomplete",
        "cleanup_scope": "connector",
        "cleanup_code": "owner_cleanup_incomplete",
    }
    phase = {
        "connector_stop": {
            "task": None,
            "complete": False,
            "result": None,
            "error_code": None,
            "cancel_requested": False,
        }
    }
    service._connector_creation_residuals[connector_key] = residual
    service._connector_cleanup_phases[owner_key] = phase

    with pytest.raises(module.ConnectorCapabilityError) as incomplete:
        await service.stop_all()

    CHECK.assertEqual(
        f"{owner_kind}_cleanup_incomplete",
        incomplete.value.detail["cleanup_code"],
    )
    CHECK.assertEqual(
        "connector_cleanup_residual",
        service._connector_creation_residuals[connector_key]["code"],
    )
    CHECK.assertIs(phase, service._connector_cleanup_phases[owner_key])

    successor_calls = 0

    def create_data_successor(connector_name):
        nonlocal successor_calls
        successor_calls += 1
        return Issue224RepairConnector()

    async def create_trading_successor(account, connector_name):
        nonlocal successor_calls
        successor_calls += 1
        return Issue224RepairConnector()

    if account_name == "__data__":
        service._create_data_connector = create_data_successor
        with pytest.raises(module.ConnectorCapabilityError):
            service.get_data_connector("manzana")
    else:
        service._create_and_initialize_trading_connector = create_trading_successor
        with pytest.raises(module.ConnectorCapabilityError):
            await service.get_trading_connector(account_name, "manzana")
    CHECK.assertEqual(0, successor_calls)
    CHECK.assertEqual(
        "connector_cleanup_residual",
        service._connector_creation_residuals[connector_key]["code"],
    )
    CHECK.assertIs(phase, service._connector_cleanup_phases[owner_key])


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal_outcome", ["success", "false", "raise"])
async def test_issue224_connector_stop_phase_keeps_exact_physical_owner(
    terminal_outcome,
):
    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)
    connector_key = ("acct", "manzana")
    owner_key = (*connector_key, "network")
    release_a = asyncio.Event()
    a_cancelled = asyncio.Event()

    class Connector(Issue224RepairConnector):
        def __init__(self, name):
            super().__init__()
            self.name = name

    connector_a = Connector("A")
    connector_b = Connector("B")

    async def stop_a():
        connector_a.stop_calls += 1
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            a_cancelled.set()
            await release_a.wait()
        if terminal_outcome == "false":
            return False
        if terminal_outcome == "raise":
            raise RuntimeError("PRIVATE-A-STOP-SECRET")
        return True

    connector_a.stop_network = stop_a
    service._trading_connectors = {"acct": {"manzana": connector_a}}
    service._advance_connector_incarnation(connector_key)
    market_key = (*connector_key, "connector")
    service._market_cleanup_owners = {
        market_key: {
            "connector": connector_b,
            "connector_preexisting": False,
        }
    }
    service._market_cleanup_residuals = {
        market_key: {
            "code": "market_cleanup_residual",
            "message": "Cleanup is incomplete",
            "connector": "manzana",
            "feature": "live_order_book",
            "reason": "cleanup_incomplete",
            "cleanup_scope": "connector",
            "cleanup_code": "cleanup_deadline_exceeded",
        }
    }
    service._market_cleanup_phases = {}
    service._market_pair_owners = {}
    service._market_residual_tasks = {}

    with pytest.raises(module.ConnectorCapabilityError):
        await service.stop_all()
    await a_cancelled.wait()
    CHECK.assertIs(connector_a, service._connector_cleanup_owners[owner_key])
    CHECK.assertEqual(1, connector_a.stop_calls)
    CHECK.assertEqual(0, connector_b.stop_calls)

    with pytest.raises(module.ConnectorCapabilityError):
        await service.stop_all()
    CHECK.assertIs(connector_a, service._connector_cleanup_owners[owner_key])
    CHECK.assertEqual(1, connector_a.stop_calls)
    CHECK.assertEqual(0, connector_b.stop_calls)

    phase_task = service._connector_cleanup_phases[owner_key]["connector_stop"][
        "task"
    ]
    release_a.set()
    await asyncio.gather(phase_task, return_exceptions=True)

    if terminal_outcome == "success":
        await service.stop_all()
        CHECK.assertEqual(1, connector_a.stop_calls)
        CHECK.assertEqual(1, connector_b.stop_calls)
        CHECK.assertEqual({}, service._trading_connectors)
        CHECK.assertEqual({}, service._connector_creation_residuals)
        CHECK.assertEqual({}, service._connector_cleanup_owners)
        CHECK.assertEqual({}, service._connector_residual_tasks)
        CHECK.assertEqual({}, service._connector_cleanup_phases)
        CHECK.assertEqual({}, service._market_cleanup_owners)
    else:
        with pytest.raises(module.ConnectorCapabilityError):
            await service.stop_all()
        CHECK.assertIs(connector_a, service._connector_cleanup_owners[owner_key])
        CHECK.assertEqual(1, connector_a.stop_calls)
        CHECK.assertEqual(0, connector_b.stop_calls)
        CHECK.assertIs(
            connector_a,
            service._trading_connectors.get("acct", {}).get("manzana"),
        )
        CHECK.assertIn(market_key, service._market_cleanup_owners)


@pytest.mark.asyncio
async def test_issue224_market_add_serializes_authenticated_key_replacement(
    monkeypatch,
):
    from services.trading_service import AccountTradingInterface

    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)
    service.secrets_manager = object()
    connector_key = ("acct", "manzana")
    connector_a = Issue224RepairConnector()
    connector_b = Issue224RepairConnector()
    connector_b._trading_pairs = []
    connector_b.order_book_tracker._trading_pairs = []
    connector_b.order_book_tracker.order_books = {}
    connector_a._update_balances = lambda: asyncio.sleep(0)
    connector_b._update_balances = lambda: asyncio.sleep(0)
    service._trading_connectors = {"acct": {"manzana": connector_a}}
    incarnation_a = service._advance_connector_incarnation(connector_key)
    service.require_capability = lambda *args, **kwargs: None
    entered_initialize = asyncio.Event()
    release_initialize = asyncio.Event()
    commit_connectors = []

    async def initialize_order_book(**kwargs):
        entered_initialize.set()
        await release_initialize.wait()
        connector_a.order_book_tracker._trading_pairs.append("BTC-USD")
        connector_a.order_book_tracker.order_books["BTC-USD"] = Issue224RepairBook()
        return True

    market_data = types.SimpleNamespace(
        initialize_order_book=initialize_order_book,
        remove_trading_pair=lambda **kwargs: asyncio.sleep(0, result=True),
    )
    interface = AccountTradingInterface(service, market_data, "acct")
    register_owner = interface._register_market_owner

    def record_commit(connector_name, trading_pair):
        commit_connectors.append(service._trading_connectors["acct"][connector_name])
        register_owner(connector_name, trading_pair)

    interface._register_market_owner = record_commit
    key_updates = []
    monkeypatch.setattr(
        module.BackendAPISecurity,
        "login_account",
        staticmethod(lambda **kwargs: True),
        raising=False,
    )
    monkeypatch.setattr(
        module.BackendAPISecurity,
        "update_connector_keys",
        staticmethod(lambda *args: key_updates.append(args)),
        raising=False,
    )
    monkeypatch.setattr(
        module.BackendAPISecurity,
        "decrypt_all",
        staticmethod(lambda **kwargs: None),
        raising=False,
    )
    monkeypatch.setattr(
        module.AllConnectorSettings,
        "get_connector_config_keys",
        staticmethod(lambda connector_name: object()),
        raising=False,
    )
    monkeypatch.setattr(
        module,
        "HummingbotAPIConfigAdapter",
        lambda config: types.SimpleNamespace(),
    )

    service._create_trading_connector = lambda *args, **kwargs: connector_b
    async def initialize_candidate(*args, **kwargs):
        return None

    service._initialize_trading_connector_candidate = initialize_candidate
    service._activate_trading_connector_candidate = initialize_candidate
    monkeypatch.setattr(
        module,
        "api_keys_from_connector_config_map",
        lambda config: {"api_key": "NEW"},
    )
    add_task = asyncio.create_task(interface.add_market("manzana", "BTC-USD"))
    await entered_initialize.wait()
    replace_task = asyncio.create_task(
        service.update_connector_keys("acct", "manzana", {"api_key": "NEW"})
    )
    await asyncio.sleep(0.01)

    try:
        CHECK.assertEqual([], key_updates)
        CHECK.assertTrue(replace_task.done())
        with pytest.raises(module.ConnectorCapabilityError):
            await replace_task
        CHECK.assertIs(connector_a, service._trading_connectors["acct"]["manzana"])
        CHECK.assertEqual(
            incarnation_a,
            service._connector_incarnation(connector_key),
        )
        release_initialize.set()
        await asyncio.wait_for(add_task, 1)
        CHECK.assertEqual([connector_a], commit_connectors)
        retry = service.update_connector_keys(
            "acct", "manzana", {"api_key": "NEW"}
        )
        CHECK.assertIs(connector_b, await asyncio.wait_for(retry, 1))
        CHECK.assertIs(connector_b, service._trading_connectors["acct"]["manzana"])
        CHECK.assertEqual(1, connector_a.stop_calls)
        CHECK.assertEqual(1, len(key_updates))
    finally:
        release_initialize.set()
        await asyncio.gather(add_task, replace_task, return_exceptions=True)


@pytest.mark.asyncio
async def test_issue224_market_transaction_blocks_inherited_and_stale_children():
    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)
    connector_key = ("acct", "manzana")
    stale_release = asyncio.Event()
    stale_rejected = asyncio.Event()

    async def ordinary_child():
        with pytest.raises(
            RuntimeError, match="market_transaction_authority_unavailable"
        ):
            async with service._market_transaction_lock(connector_key):
                CHECK.fail("an inherited child received mutation authority")

    async def stale_child():
        await stale_release.wait()
        with pytest.raises(module.ConnectorCapabilityError):
            async with service._market_transaction_lock(connector_key):
                CHECK.fail("a stale child queued through an active root lease")
        stale_rejected.set()

    async with service._market_transaction_lock(connector_key) as first_token:
        first_owner = asyncio.current_task()
        ordinary = asyncio.create_task(ordinary_child())
        stale = asyncio.create_task(stale_child())
        await ordinary
        CHECK.assertIs(first_owner, first_token.owner_task)
        CHECK.assertTrue(service._market_locks[connector_key].locked())
        async with service._market_transaction_lock(connector_key) as nested:
            CHECK.assertIs(first_token, nested)

    CHECK.assertIs(first_token.active, False)
    CHECK.assertIsNone(first_token.owner_task)

    async with service._market_transaction_lock(connector_key) as second_token:
        stale_release.set()
        await asyncio.sleep(0)
        CHECK.assertIs(asyncio.current_task(), second_token.owner_task)
        CHECK.assertTrue(stale_rejected.is_set())

    await asyncio.wait_for(stale, 1)
    CHECK.assertIs(second_token.active, False)
    CHECK.assertIsNone(second_token.owner_task)
    CHECK.assertEqual({}, service._market_lock_users)
    CHECK.assertEqual({}, service._market_locks)


@pytest.mark.asyncio
async def test_issue224_nested_lease_survives_out_of_order_error_cancel_and_gc():
    import gc
    import weakref

    for outcome in ("error", "cancel"):
        module, registry_type = _load_unified_service_module()
        service = _final_boundary_service(module, registry_type)
        connector_key = ("acct", "manzana")
        child_release = asyncio.Event()
        grandchild_release = asyncio.Event()
        grandchild_ready = asyncio.Event()
        grandchild_holder = {}
        contender_detail = {}

        async def grandchild_operation():
            await grandchild_release.wait()

        async def child_operation():
            grandchild_task = asyncio.create_task(grandchild_operation())
            CHECK.assertTrue(
                service._retain_market_transaction_descendant(
                    connector_key, grandchild_task
                )
            )
            grandchild_holder["task"] = grandchild_task
            grandchild_ready.set()
            if outcome == "error":
                raise RuntimeError("expected child failure")
            await child_release.wait()

        async def contender():
            with pytest.raises(module.ConnectorCapabilityError) as blocked:
                async with service._market_transaction_lock(connector_key):
                    CHECK.fail("a retained lease admitted another mutator")
            contender_detail.update(blocked.value.detail)

        async with service._market_transaction_lock(connector_key) as token:
            child = asyncio.create_task(child_operation())
            child_ref = weakref.ref(child)
            CHECK.assertTrue(
                service._retain_market_transaction_descendant(
                    connector_key, child
                )
            )
            await grandchild_ready.wait()
            grandchild = grandchild_holder.pop("task")
            grandchild_ref = weakref.ref(grandchild)
            if outcome == "cancel":
                child.cancel()
            await asyncio.gather(child, return_exceptions=True)
            await asyncio.sleep(0)
            CHECK.assertNotIn(child, token.lease.frames)
            CHECK.assertIn(grandchild, token.lease.frames)
            with pytest.raises(
                RuntimeError,
                match="market_transaction_authority_unavailable",
            ):
                async with service._market_transaction_lock(connector_key):
                    CHECK.fail("the root resumed while a descendant was live")

        CHECK.assertTrue(token.active)
        CHECK.assertIsNone(token.owner_task)
        contender_task = asyncio.create_task(contender())
        await asyncio.wait_for(contender_task, 1)
        CHECK.assertEqual(
            "connector_cleanup_residual", contender_detail["code"]
        )
        CHECK.assertTrue(service._market_locks[connector_key].locked())
        CHECK.assertEqual(1, service._market_lock_users[connector_key])

        grandchild_release.set()
        for _ in range(20):
            if not token.active:
                break
            await asyncio.sleep(0)
        CHECK.assertFalse(token.active)
        async with service._market_transaction_lock(connector_key):
            pass
        CHECK.assertEqual({}, service._market_lock_users)
        CHECK.assertEqual({}, service._market_locks)
        del child
        del grandchild
        await asyncio.sleep(0)
        gc.collect()
        CHECK.assertIsNone(child_ref())
        CHECK.assertIsNone(grandchild_ref())


@pytest.mark.asyncio
async def test_issue224_cancelled_public_stop_retains_lease_against_contender():
    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)
    connector_key = ("acct", "manzana")
    connector = Issue224RepairConnector()
    stop_entered = asyncio.Event()
    stop_release = asyncio.Event()
    stop_terminal = asyncio.Event()
    contender_detail = {}

    async def stop_network():
        connector.stop_calls += 1
        stop_entered.set()
        try:
            await stop_release.wait()
        except asyncio.CancelledError:
            await stop_release.wait()
        finally:
            stop_terminal.set()

    connector.stop_network = stop_network
    service._trading_connectors = {"acct": {"manzana": connector}}
    service._advance_connector_incarnation(connector_key)

    async def contender():
        with pytest.raises(module.ConnectorCapabilityError) as blocked:
            async with service._market_transaction_lock(connector_key):
                CHECK.fail("a retained stop admitted another mutator")
        contender_detail.update(blocked.value.detail)

    parent_task = asyncio.create_task(
        service.stop_trading_connector("acct", "manzana")
    )
    await stop_entered.wait()
    parent_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(parent_task, 0.2)

    contender_task = asyncio.create_task(contender())
    await asyncio.wait_for(contender_task, 1)
    CHECK.assertFalse(stop_terminal.is_set())
    CHECK.assertEqual(
        "connector_cleanup_residual", contender_detail["code"]
    )
    CHECK.assertTrue(service._market_locks[connector_key].locked())
    CHECK.assertEqual(1, service._market_lock_users[connector_key])

    stop_release.set()
    await asyncio.wait_for(stop_terminal.wait(), 1)
    CHECK.assertEqual(1, connector.stop_calls)
    for _ in range(20):
        if not service._market_locks:
            break
        await asyncio.sleep(0)
    CHECK.assertEqual({}, service._market_lock_users)
    CHECK.assertEqual({}, service._market_locks)


@pytest.mark.asyncio
async def test_issue224_cleanup_deadline_skips_local_mutation_until_physical_terminal():
    from services.trading_service import AccountTradingInterface

    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)
    connector_key = ("acct", "manzana")
    connector = Issue224RepairConnector()
    connector._trading_pairs.append("BTC-USD")
    connector.order_book_tracker._trading_pairs.append("BTC-USD")
    connector.order_book_tracker.order_books["BTC-USD"] = Issue224RepairBook()
    stop_entered = asyncio.Event()
    stop_release = asyncio.Event()
    stop_terminal = asyncio.Event()

    async def stop_network():
        connector.stop_calls += 1
        try:
            stop_entered.set()
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await stop_release.wait()
        finally:
            stop_terminal.set()

    connector.stop_network = stop_network
    service._trading_connectors = {"acct": {"manzana": connector}}
    incarnation = service._advance_connector_incarnation(connector_key)
    service.require_capability = lambda *args, **kwargs: None

    async def controlled_stop(account_name, connector_name):
        await service._run_owner_operation(
            (*connector_key, "network"),
            "connector_stop",
            connector.stop_network,
            timeout=1,
        )
        return None

    service._stop_trading_connector_already_locked = controlled_stop
    market_data = types.SimpleNamespace(
        remove_trading_pair=lambda **kwargs: asyncio.sleep(0, result=True)
    )
    interface = AccountTradingInterface(service, market_data, "acct")
    remove_calls = 0
    original_remove = interface._remove_owned_pair_state

    async def record_remove(*args):
        nonlocal remove_calls
        remove_calls += 1
        CHECK.assertTrue(stop_terminal.is_set())
        return await original_remove(*args)

    interface._remove_owned_pair_state = record_remove
    started = asyncio.get_running_loop().time()
    try:
        async with service._market_transaction_lock(connector_key):
            detail = await interface._complete_market_cleanup(
                "manzana", "BTC-USD", connector, False, incarnation
            )
        elapsed = asyncio.get_running_loop().time() - started

        CHECK.assertLess(elapsed, 0.25)
        CHECK.assertEqual("market_cleanup_residual", detail.code)
        CHECK.assertEqual("cleanup_deadline_exceeded", detail.cleanup_code)
        CHECK.assertEqual(0, remove_calls)
        CHECK.assertFalse(stop_terminal.is_set())
        CHECK.assertTrue(service._market_locks[connector_key].locked())
        CHECK.assertTrue(stop_entered.is_set())
    finally:
        stop_release.set()
        await asyncio.wait_for(stop_terminal.wait(), 1)
    for _ in range(20):
        if not service._market_locks:
            break
        await asyncio.sleep(0)
    CHECK.assertEqual({}, service._market_lock_users)
    CHECK.assertEqual({}, service._market_locks)


@pytest.mark.asyncio
async def test_issue224_retention_failure_is_owned_and_fails_closed():
    module, registry_type = _load_unified_service_module()
    connector_key = ("acct", "manzana")

    success_service = _final_boundary_service(module, registry_type)
    success_connector = Issue224RepairConnector()
    success_service._trading_connectors = {
        "acct": {"manzana": success_connector}
    }
    success_service._advance_connector_incarnation(connector_key)
    async with success_service._market_transaction_lock(connector_key):
        CHECK.assertIs(
            True,
            await success_service.stop_trading_connector("acct", "manzana"),
        )
    CHECK.assertEqual(1, success_connector.stop_calls)
    CHECK.assertEqual({}, success_service._market_locks)

    failed_service = _final_boundary_service(module, registry_type)
    failed_connector = Issue224RepairConnector()
    failed_service._trading_connectors = {
        "acct": {"manzana": failed_connector}
    }
    failed_service._advance_connector_incarnation(connector_key)
    failed_service._retain_market_transaction_descendant = (
        lambda connector_key_arg, task: False
    )

    async with failed_service._market_transaction_lock(connector_key):
        with pytest.raises(module.ConnectorCapabilityError) as failure:
            await asyncio.wait_for(
                failed_service.stop_trading_connector("acct", "manzana"),
                0.2,
            )
        CHECK.assertEqual(
            "connector_cleanup_residual", failure.value.detail["code"]
        )
    CHECK.assertEqual(0, failed_connector.stop_calls)
    CHECK.assertEqual({}, failed_service._market_locks)
    CHECK.assertEqual({}, failed_service._market_lock_users)


@pytest.mark.asyncio
async def test_issue224_live_residual_preflight_is_bounded_for_add_stop_and_keys(
    monkeypatch,
):
    from services.trading_service import AccountTradingInterface

    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)
    service.secrets_manager = object()
    connector_key = ("acct", "manzana")
    residual_key = (*connector_key, "pair", "BTC-USD")
    residual = {
        "code": "market_cleanup_residual",
        "message": "Cleanup is incomplete for 'manzana/BTC-USD'",
        "connector": "manzana",
        "feature": "live_order_book",
        "trading_pair": "BTC-USD",
        "reason": "cleanup_incomplete",
        "cleanup_scope": "pair",
        "cleanup_code": "cleanup_deadline_exceeded",
    }
    release = asyncio.Event()

    async def physical_owner():
        await release.wait()

    async with service._market_transaction_lock(connector_key):
        physical_task = asyncio.create_task(physical_owner())
        CHECK.assertTrue(
            service._retain_market_transaction_descendant(
                connector_key, physical_task
            )
        )
        service._market_cleanup_residuals[residual_key] = residual

    market_data = types.SimpleNamespace()
    interface = AccountTradingInterface(service, market_data, "acct")
    interface._require_capability = lambda *args, **kwargs: None

    writes = []
    monkeypatch.setattr(
        module.BackendAPISecurity,
        "login_account",
        staticmethod(lambda **kwargs: True),
        raising=False,
    )
    monkeypatch.setattr(
        module.BackendAPISecurity,
        "update_connector_keys",
        staticmethod(lambda *args: writes.append(args)),
        raising=False,
    )
    monkeypatch.setattr(
        module.AllConnectorSettings,
        "get_connector_config_keys",
        staticmethod(lambda connector_name: object()),
        raising=False,
    )
    monkeypatch.setattr(
        module,
        "HummingbotAPIConfigAdapter",
        lambda config: types.SimpleNamespace(),
    )

    for operation in (
        interface.add_market("manzana", "BTC-USD"),
        service.stop_trading_connector("acct", "manzana"),
        service.update_connector_keys(
            "acct", "manzana", {"api_key": "NEW"}
        ),
    ):
        with pytest.raises(module.ConnectorCapabilityError) as blocked:
            await asyncio.wait_for(operation, 0.2)
        CHECK.assertEqual("market_cleanup_residual", blocked.value.detail["code"])
        CHECK.assertEqual(1, service._market_lock_users[connector_key])

    CHECK.assertEqual([], writes)
    release.set()
    await physical_task
    for _ in range(20):
        if not service._market_locks:
            break
        await asyncio.sleep(0)
    CHECK.assertEqual({}, service._market_locks)
    CHECK.assertEqual({}, service._market_leases)


@pytest.mark.asyncio
async def test_issue224_retained_frame_never_receives_public_stop_authority():
    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)
    connector_key = ("acct", "manzana")
    connector = Issue224RepairConnector()
    service._trading_connectors = {"acct": {"manzana": connector}}
    service._advance_connector_incarnation(connector_key)
    invoke = asyncio.Event()

    async def retained_public_call():
        await invoke.wait()
        with pytest.raises(
            RuntimeError, match="market_transaction_authority_unavailable"
        ):
            await service.stop_trading_connector("acct", "manzana")

    async with service._market_transaction_lock(connector_key):
        retained_task = asyncio.create_task(retained_public_call())
        CHECK.assertTrue(
            service._retain_market_transaction_descendant(
                connector_key, retained_task
            )
        )
        invoke.set()
        await retained_task

    CHECK.assertEqual(0, connector.stop_calls)
    CHECK.assertIs(connector, service._trading_connectors["acct"]["manzana"])
    CHECK.assertEqual({}, service._market_locks)


@pytest.mark.asyncio
async def test_issue224_preacquire_cancellation_reaps_guardians_and_users():
    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)
    service._ensure_connector_cleanup_state()
    connector_key = ("acct", "manzana")
    contenders_started = asyncio.Event()
    raw_lock = asyncio.Lock()
    await raw_lock.acquire()
    service._market_locks[connector_key] = raw_lock

    async def contender():
        contenders_started.set()
        async with service._market_transaction_lock(connector_key):
            CHECK.fail("a queued contender acquired before cancellation")

    try:
        contenders = [asyncio.create_task(contender()) for _ in range(5)]
        await contenders_started.wait()
        for _ in range(50):
            if service._market_lock_users.get(connector_key) == 5:
                break
            await asyncio.sleep(0)
        CHECK.assertEqual(5, service._market_lock_users[connector_key])
        for task in contenders:
            task.cancel()
        await asyncio.gather(*contenders, return_exceptions=True)
        CHECK.assertEqual({}, service._market_lock_users)
        CHECK.assertEqual({}, service._market_leases)
        CHECK.assertEqual({}, service._market_locks)
    finally:
        if raw_lock.locked():
            raw_lock.release()


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal_outcome", ["success", "false", "raise"])
async def test_issue224_connector_cleanup_retry_uses_physical_terminal_truth(
    terminal_outcome,
):
    from services.trading_service import AccountTradingInterface

    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)
    connector_key = ("acct", "manzana")
    connector = Issue224RepairConnector()
    service._trading_connectors = {"acct": {"manzana": connector}}
    incarnation = service._advance_connector_incarnation(connector_key)
    entered = asyncio.Event()
    release = asyncio.Event()
    terminal = asyncio.Event()

    async def stop_network():
        connector.stop_calls += 1
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await release.wait()
        finally:
            terminal.set()
        if terminal_outcome == "false":
            return False
        if terminal_outcome == "raise":
            raise RuntimeError("PRIVATE-PHYSICAL-STOP")
        return True

    connector.stop_network = stop_network
    interface = AccountTradingInterface(
        service, types.SimpleNamespace(), "acct"
    )

    async with service._market_transaction_lock(connector_key):
        first = await interface._complete_market_cleanup(
            "manzana", "BTC-USD", connector, False, incarnation
        )
    await entered.wait()
    CHECK.assertEqual("cleanup_deadline_exceeded", first.cleanup_code)
    CHECK.assertTrue(service._market_locks[connector_key].locked())

    release.set()
    await terminal.wait()
    for _ in range(50):
        if not service._market_locks:
            break
        await asyncio.sleep(0)
    CHECK.assertEqual(
        {},
        service._market_locks,
        (service._market_leases, service._connector_residual_tasks),
    )
    if terminal_outcome == "success":
        async with interface._market_transaction("manzana", "BTC-USD"):
            await interface._retry_market_cleanup_residual(
                "manzana", "BTC-USD"
            )
        CHECK.assertEqual({}, interface._cleanup_residuals)
        CHECK.assertEqual({}, interface._cleanup_phases)
        CHECK.assertEqual({}, interface._residual_tasks)
    else:
        async with interface._market_transaction("manzana", "BTC-USD"):
            with pytest.raises(module.ConnectorCapabilityError):
                await interface._retry_market_cleanup_residual(
                    "manzana", "BTC-USD"
                )
        CHECK.assertTrue(interface._cleanup_residuals)
        CHECK.assertTrue(service._connector_cleanup_phases)
    CHECK.assertEqual(1, connector.stop_calls)


@pytest.mark.asyncio
async def test_issue224_pair_cache_and_balance_mutators_reject_retained_lease():
    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)
    connector_key = ("acct", "manzana")
    connector = Issue224RepairConnector()
    connector._trading_pairs.append("BTC-USD")
    connector.order_book_tracker._trading_pairs.append("BTC-USD")
    connector.order_book_tracker.order_books["BTC-USD"] = Issue224RepairBook()
    service._trading_connectors = {"acct": {"manzana": connector}}
    service._advance_connector_incarnation(connector_key)
    release = asyncio.Event()
    balance_calls = 0

    async def physical_owner():
        await release.wait()

    async def update_balances():
        nonlocal balance_calls
        balance_calls += 1

    connector._update_balances = update_balances
    async with service._market_transaction_lock(connector_key):
        physical_task = asyncio.create_task(physical_owner())
        CHECK.assertTrue(
            service._retain_market_transaction_descendant(
                connector_key, physical_task
            )
        )

    before_pairs = list(connector._trading_pairs)
    for operation in (
        service.initialize_order_book(
            "manzana", "ETH-USD", account_name="acct", timeout=0
        ),
        service.remove_trading_pair(
            "manzana", "BTC-USD", account_name="acct"
        ),
        service.refresh_connector_state(connector, "manzana", "acct"),
    ):
        with pytest.raises(module.ConnectorCapabilityError):
            await asyncio.wait_for(operation, 0.2)
    with pytest.raises(module.ConnectorCapabilityError):
        service.clear_trading_connector("acct", "manzana")

    CHECK.assertEqual(before_pairs, connector._trading_pairs)
    CHECK.assertEqual(0, balance_calls)
    CHECK.assertIs(connector, service._trading_connectors["acct"]["manzana"])
    CHECK.assertEqual(1, service._market_lock_users[connector_key])

    release.set()
    await physical_task


def test_issue224_account_routes_preserve_typed_capability_residuals():
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from models.connectors import (
        CapabilityErrorDetail,
        CapabilityErrorResponse,
    )
    from routers import accounts as accounts_router
    from services.connector_capabilities import ConnectorCapabilityError

    detail = CapabilityErrorDetail(
        code="connector_cleanup_residual",
        message="Connector cleanup is incomplete for 'acct:manzana'",
        connector="manzana",
        feature="connector_cleanup",
        reason="cleanup_incomplete",
        cleanup_scope="connector",
        cleanup_code="cleanup_deadline_exceeded",
    )

    class Accounts:
        async def add_credentials(self, *args, **kwargs):
            raise ConnectorCapabilityError(detail)

        async def delete_credentials(self, *args, **kwargs):
            raise ConnectorCapabilityError(detail)

        async def delete_account(self, *args, **kwargs):
            raise ConnectorCapabilityError(detail)

    app = FastAPI()
    app.include_router(accounts_router.router)
    app.dependency_overrides[accounts_router.get_accounts_service] = Accounts
    client = TestClient(app)
    responses = (
        client.post(
            "/accounts/add-credential/acct/manzana",
            json={"api_key": "SECRET"},
        ),
        client.post("/accounts/delete-credential/acct/manzana"),
        client.post("/accounts/delete-account", params={"account_name": "acct"}),
    )
    for response in responses:
        CHECK.assertEqual(400, response.status_code, response.text)
        CHECK.assertEqual(detail.model_dump(exclude_none=True), response.json()["detail"])
        CapabilityErrorResponse.model_validate(response.json())

    paths = app.openapi()["paths"]
    for path in (
        "/accounts/add-credential/{account_name}/{connector_name}",
        "/accounts/delete-credential/{account_name}/{connector_name}",
        "/accounts/delete-account",
    ):
        response_schema = paths[path]["post"]["responses"]["400"][
            "content"
        ]["application/json"]["schema"]
        CHECK.assertEqual(
            "#/components/schemas/CapabilityErrorResponse",
            response_schema["$ref"],
        )


@pytest.mark.asyncio
async def test_issue224_delete_credentials_commits_only_after_terminal_stop(
    monkeypatch,
):
    from contextlib import asynccontextmanager

    from models.connectors import CapabilityErrorDetail
    from services.connector_capabilities import ConnectorCapabilityError

    accounts_module = _load_accounts_service_module()
    events = []
    file_state = {"exists": True, "content": "OLD-CREDENTIAL"}
    detail = CapabilityErrorDetail(
        code="connector_cleanup_residual",
        message="Connector cleanup is incomplete for 'acct:manzana'",
        connector="manzana",
        feature="connector_cleanup",
        reason="cleanup_incomplete",
        cleanup_scope="connector",
        cleanup_code="cleanup_deadline_exceeded",
    )

    class ConnectorService:
        fail = True

        @asynccontextmanager
        async def credential_transaction(self, account_name, connector_name):
            events.append("enter")
            try:
                yield
            finally:
                events.append("exit")

        async def _stop_trading_connector_already_locked(
            self, account_name, connector_name
        ):
            events.append("stop")
            if self.fail:
                return detail
            return None

    connector_service = ConnectorService()
    service = object.__new__(accounts_module.AccountsService)
    service._connector_service = connector_service
    service.accounts_state = {"acct": {"manzana": ["balance"]}}

    monkeypatch.setattr(
        accounts_module.fs_util,
        "read_file",
        lambda path: file_state["content"],
        raising=False,
    )

    monkeypatch.setattr(
        accounts_module.fs_util,
        "path_exists",
        lambda path: file_state["exists"],
        raising=False,
    )

    def delete_file(**kwargs):
        events.append("delete")
        file_state["exists"] = False

    monkeypatch.setattr(
        accounts_module.fs_util,
        "delete_file",
        delete_file,
        raising=False,
    )

    def restore_file(path, content):
        events.append("restore")
        file_state.update(exists=True, content=content)

    monkeypatch.setattr(
        accounts_module.fs_util,
        "ensure_file_and_dump_text",
        restore_file,
        raising=False,
    )

    with pytest.raises(ConnectorCapabilityError):
        await service.delete_credentials("acct", "manzana")
    CHECK.assertEqual(["enter", "delete", "stop", "restore", "exit"], events)
    CHECK.assertTrue(file_state["exists"])
    CHECK.assertEqual(["balance"], service.accounts_state["acct"]["manzana"])

    connector_service.fail = False
    await service.delete_credentials("acct", "manzana")
    CHECK.assertEqual(
        [
            "enter",
            "delete",
            "stop",
            "restore",
            "exit",
            "enter",
            "delete",
            "stop",
            "exit",
        ],
        events,
    )
    CHECK.assertFalse(file_state["exists"])
    CHECK.assertNotIn("manzana", service.accounts_state["acct"])


@pytest.mark.asyncio
async def test_issue224_waiting_guardian_observes_root_residual_before_release():
    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)
    service._ensure_connector_cleanup_state()
    connector_key = ("acct", "manzana")
    publish = asyncio.Event()
    published = asyncio.Event()
    release = asyncio.Event()

    residual = {
        "code": "connector_cleanup_residual",
        "message": "Connector cleanup is incomplete for 'acct:manzana'",
        "connector": "manzana",
        "feature": "connector_cleanup",
        "reason": "cleanup_incomplete",
        "cleanup_scope": "connector",
        "cleanup_code": "connector_stop_pending",
    }

    async def holder():
        async with service._market_transaction_lock(connector_key):
            await publish.wait()
            service._connector_creation_residuals[connector_key] = residual
            notifier = getattr(service, "_signal_market_state_change", None)
            if notifier is not None:
                notifier(connector_key)
            published.set()
            await release.wait()

    async def contender():
        async with service._market_transaction_lock(connector_key):
            CHECK.fail("a queued contender acquired through a live residual")

    holder_task = asyncio.create_task(holder())
    for _ in range(100):
        if service._market_lock_users.get(connector_key) == 1:
            break
        await asyncio.sleep(0)
    contender_task = asyncio.create_task(contender())
    for _ in range(100):
        if service._market_lock_users.get(connector_key) == 2:
            break
        await asyncio.sleep(0)
    CHECK.assertEqual(2, service._market_lock_users[connector_key])

    publish.set()
    await published.wait()
    with pytest.raises(module.ConnectorCapabilityError) as blocked:
        await asyncio.wait_for(contender_task, 0.2)
    CHECK.assertEqual("connector_stop_pending", blocked.value.detail["cleanup_code"])
    CHECK.assertFalse(release.is_set())

    release.set()
    await holder_task
    for _ in range(100):
        if not service._market_locks:
            break
        await asyncio.sleep(0)
    CHECK.assertEqual({}, service._market_lock_users)
    CHECK.assertEqual({}, service._market_locks)


@pytest.mark.asyncio
async def test_issue224_active_root_blocks_foreign_sync_publish_and_clear():
    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)
    service._ensure_connector_cleanup_state()
    data_key = ("__data__", "manzana")
    trading_key = ("acct", "manzana")
    old_connector = Issue224RepairConnector()
    service._trading_connectors = {"acct": {"manzana": old_connector}}
    service._create_data_connector = lambda name: Issue224RepairConnector()
    service._validate_instance_capabilities = lambda *args: None
    mutate = asyncio.Event()
    mutations_done = asyncio.Event()
    failures = []

    async def foreign_mutator():
        await mutate.wait()
        for operation in (
            lambda: service.get_data_connector("manzana"),
            lambda: service.clear_trading_connector("acct", "manzana"),
        ):
            try:
                operation()
            except (module.ConnectorCapabilityError, RuntimeError):
                failures.append(True)
        mutations_done.set()

    foreign_task = asyncio.create_task(foreign_mutator())
    async with service._market_transaction_lock(data_key):
        async with service._market_transaction_lock(trading_key):
            mutate.set()
            await mutations_done.wait()
            CHECK.assertNotIn("manzana", service._data_connectors)
            CHECK.assertIs(
                old_connector,
                service._trading_connectors["acct"]["manzana"],
            )
    await foreign_task
    CHECK.assertEqual([True, True], failures)


@pytest.mark.asyncio
async def test_issue224_repeated_caller_cancellation_cannot_interrupt_guardian_pruning():
    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)
    service._ensure_connector_cleanup_state()
    connector_key = ("acct", "manzana")
    holder_release = asyncio.Event()

    async def holder():
        async with service._market_transaction_lock(connector_key):
            await holder_release.wait()

    async def contender():
        async with service._market_transaction_lock(connector_key):
            CHECK.fail("cancelled contender acquired the lock")

    holder_task = asyncio.create_task(holder())
    for _ in range(100):
        if service._market_lock_users.get(connector_key) == 1:
            break
        await asyncio.sleep(0)
    contender_task = asyncio.create_task(contender())
    for _ in range(100):
        if service._market_lock_users.get(connector_key) == 2:
            break
        await asyncio.sleep(0)
    CHECK.assertEqual(2, service._market_lock_users[connector_key])

    for _ in range(8):
        contender_task.cancel()
        await asyncio.sleep(0)
    await asyncio.gather(contender_task, return_exceptions=True)
    CHECK.assertEqual(1, service._market_lock_users[connector_key])
    CHECK.assertTrue(service._market_locks[connector_key].locked())

    holder_release.set()
    await holder_task
    for _ in range(100):
        if not service._market_locks:
            break
        await asyncio.sleep(0)
    CHECK.assertEqual({}, service._market_lock_users)
    CHECK.assertEqual({}, service._market_locks)
    CHECK.assertEqual({}, service._market_leases)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "terminal_outcome", ["success", "false", "raise", "cancel"]
)
async def test_issue224_physical_wrapper_reconciles_before_lease_release(
    terminal_outcome,
):
    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)
    service._ensure_connector_cleanup_state()
    connector_key = ("acct", "manzana")
    connector = Issue224RepairConnector()
    service._trading_connectors = {"acct": {"manzana": connector}}
    service._advance_connector_incarnation(connector_key)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def stop_network():
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await release.wait()
            if terminal_outcome == "cancel":
                raise
        if terminal_outcome == "false":
            return False
        if terminal_outcome == "raise":
            raise RuntimeError("PRIVATE-TERMINAL-STOP")
        return True

    connector.stop_network = stop_network
    public_stop = asyncio.create_task(
        service.stop_trading_connector("acct", "manzana")
    )
    await entered.wait()
    public_stop.cancel()
    await asyncio.gather(public_stop, return_exceptions=True)
    CHECK.assertTrue(service._market_locks[connector_key].locked())

    release.set()
    for _ in range(200):
        if not service._market_locks:
            break
        await asyncio.sleep(0)
    CHECK.assertEqual({}, service._market_locks)
    if terminal_outcome == "success":
        CHECK.assertIsNot(
            connector,
            service._trading_connectors.get("acct", {}).get("manzana"),
        )
        CHECK.assertNotIn(connector_key, service._connector_creation_residuals)
        for registry in (
            service._connector_cleanup_owners,
            service._connector_cleanup_phases,
            service._connector_residual_tasks,
            service._market_cleanup_residuals,
            service._market_cleanup_owners,
            service._market_residual_tasks,
            service._market_cleanup_phases,
        ):
            CHECK.assertFalse(
                any(key[:2] == connector_key for key in registry), registry
            )
        CHECK.assertNotIn(connector_key, service._connector_incarnations)

        replacement = Issue224RepairConnector()

        async def create_replacement(account_name, connector_name):
            return replacement

        service._create_and_initialize_trading_connector = create_replacement
        CHECK.assertIs(
            replacement,
            await service.get_trading_connector("acct", "manzana"),
        )
        CHECK.assertEqual(0, replacement.stop_calls)
    else:
        CHECK.assertIs(
            connector,
            service._trading_connectors.get("acct", {}).get("manzana"),
        )
        expected_codes = {
            "false": "connector_stop_returned_false",
            "raise": "connector_stop_raised",
            "cancel": "connector_stop_cancelled",
        }
        CHECK.assertEqual(
            expected_codes[terminal_outcome],
            service._connector_creation_residuals[connector_key]["cleanup_code"],
        )


def test_issue224_account_route_all_400_bodies_match_capability_contract():
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from models.connectors import CapabilityErrorResponse
    from routers import accounts as accounts_router
    from services.accounts_service import validate_safe_name

    class Accounts:
        async def delete_account(self, account_name):
            raise AssertionError("delete_account should not run")

        async def delete_credentials(self, account_name, connector_name):
            validate_safe_name(account_name, "account name")

        async def add_credentials(self, account_name, connector_name, credentials):
            validate_safe_name(account_name, "account name")
            raise RuntimeError("PRIVATE-CREDENTIAL-FAILURE")

    app = FastAPI()
    app.include_router(accounts_router.router)
    app.dependency_overrides[accounts_router.get_accounts_service] = Accounts
    client = TestClient(app)
    responses = (
        client.post("/accounts/delete-account", params={"account_name": "bad.name"}),
        client.post("/accounts/delete-account", params={"account_name": "master_account"}),
        client.post("/accounts/delete-credential/bad.name/manzana"),
        client.post(
            "/accounts/add-credential/acct/manzana",
            json={"api_key": "SECRET"},
        ),
    )
    for response in responses:
        CHECK.assertEqual(400, response.status_code, response.text)
        CapabilityErrorResponse.model_validate(response.json())
        CHECK.assertIsInstance(response.json()["detail"], dict)


@pytest.mark.asyncio
async def test_issue224_credential_refresh_rollback_precedes_later_update(
    monkeypatch,
):
    from contextlib import asynccontextmanager

    accounts_module = _load_accounts_service_module()
    transaction_lock = asyncio.Lock()
    refresh_entered = asyncio.Event()
    release_refresh = asyncio.Event()
    file_state = {"content": "OLD"}
    cache_state = {"connector": "OLD"}

    class ConnectorService:
        @asynccontextmanager
        async def credential_transaction(self, account_name, connector_name):
            async with transaction_lock:
                yield

        async def stage_connector_keys_already_locked(
            self, account_name, connector_name, credentials
        ):
            return {
                "connector": types.SimpleNamespace(
                    value=credentials["api_key"]
                ),
                "committed": False,
            }

        async def commit_staged_connector_keys_already_locked(
            self, account_name, connector_name, stage
        ):
            value = stage["connector"].value
            file_state["content"] = value
            cache_state["connector"] = value
            stage["committed"] = True
            return stage["connector"]

        async def discard_staged_connector_already_locked(
            self, account_name, connector_name, stage
        ):
            stage["discarded"] = True

    service = object.__new__(accounts_module.AccountsService)
    service._connector_service = ConnectorService()
    service.accounts_state = {"acct": {"manzana": ["OLD"]}}

    monkeypatch.setattr(
        accounts_module.fs_util,
        "path_exists",
        lambda path: True,
        raising=False,
    )
    monkeypatch.setattr(
        accounts_module.fs_util,
        "read_file",
        lambda path: file_state["content"],
        raising=False,
    )
    monkeypatch.setattr(
        accounts_module.fs_util,
        "ensure_file_and_dump_text",
        lambda path, content: file_state.__setitem__("content", content),
        raising=False,
    )

    async def staged_state(connector, *args, **kwargs):
        if connector.value == "BAD":
            refresh_entered.set()
            await release_refresh.wait()
            raise RuntimeError("refresh_failed")
        return [connector.value]

    service._get_connector_tokens_info = staged_state
    rejected = asyncio.create_task(
        service.add_credentials("acct", "manzana", {"api_key": "BAD"})
    )
    await refresh_entered.wait()
    successor = asyncio.create_task(
        service.add_credentials("acct", "manzana", {"api_key": "NEW"})
    )
    await asyncio.sleep(0)
    CHECK.assertFalse(successor.done())
    release_refresh.set()
    with pytest.raises(RuntimeError, match="refresh_failed"):
        await rejected
    await successor
    CHECK.assertEqual("NEW", file_state["content"])
    CHECK.assertEqual("NEW", cache_state["connector"])
    CHECK.assertEqual(["NEW"], service.accounts_state["acct"]["manzana"])


@pytest.mark.asyncio
async def test_issue224_balance_refresh_retries_identity_and_commits_under_authority():
    from contextlib import asynccontextmanager
    from decimal import Decimal

    from models.connectors import CapabilityErrorDetail
    from services.connector_capabilities import ConnectorCapabilityError

    accounts_module = _load_accounts_service_module()

    class Connector:
        in_flight_orders = {}

        def __init__(self, token):
            self.token = token
            self.balance_calls = 0

        def _set_current_timestamp(self, timestamp):
            pass

        async def _update_balances(self):
            self.balance_calls += 1

        def get_all_balances(self):
            return {"USDT": Decimal("2")}

        def get_available_balance(self, token):
            return Decimal("2")

    old = Connector("OLD")
    current = Connector("NEW")

    class ConnectorService:
        def __init__(self):
            self.authority = False

        @asynccontextmanager
        async def connector_state_authority(
            self, connector, connector_name, account_name
        ):
            if connector is not current:
                raise ConnectorCapabilityError(
                    CapabilityErrorDetail(
                        code="connector_incarnation_changed",
                        message="Connector changed while refreshing state",
                        connector=connector_name,
                        feature="connector_state",
                        reason="connector_incarnation_changed",
                    )
                )
            self.authority = True
            try:
                yield
                CHECK.assertEqual(
                    2.0,
                    service.accounts_state["acct"]["manzana"][0]["units"],
                )
            finally:
                self.authority = False

        async def _update_connector_state(
            self, connector, connector_name, account_name
        ):
            CHECK.assertTrue(self.authority)
            await connector._update_balances()

        def get_account_connectors(self, account_name):
            return {"manzana": current}

    connector_service = ConnectorService()
    service = object.__new__(accounts_module.AccountsService)
    service._connector_service = connector_service
    service._market_data_service = types.SimpleNamespace(
        get_rate_for_connector=lambda *args: None
    )
    service.accounts_state = {"acct": {}}
    service.require_capability = lambda *args, **kwargs: None
    service._is_perpetual_connector = lambda connector: False

    result = await service._refresh_account_connector_state(
        old, "manzana", "acct"
    )
    CHECK.assertEqual(0, old.balance_calls)
    CHECK.assertEqual(1, current.balance_calls)
    CHECK.assertEqual(result, service.accounts_state["acct"]["manzana"])
    CHECK.assertFalse(connector_service.authority)


@pytest.mark.asyncio
async def test_issue224_credential_candidate_is_fully_staged_and_exactly_discarded(
    monkeypatch,
):
    from models.connectors import CapabilityErrorDetail
    from services.connector_capabilities import ConnectorCapabilityError

    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)
    service.secrets_manager = object()
    service._conn_settings = {"manzana": object()}
    old_connector = object()
    candidate = object()
    old_indices = {
        "orders": object(),
        "funding": object(),
        "metrics": object(),
    }
    candidate_indices = {
        "orders": object(),
        "funding": object(),
        "metrics": object(),
    }
    cache_key = "acct:manzana"
    connector_key = ("acct", "manzana")
    service._trading_connectors = {"acct": {"manzana": old_connector}}
    service._orders_recorders[cache_key] = old_indices["orders"]
    service._funding_recorders[cache_key] = old_indices["funding"]
    service._metrics_collectors[cache_key] = old_indices["metrics"]
    service._advance_connector_incarnation(connector_key)
    events = []
    file_state = {"exists": True, "content": "OLD"}
    settings_state = {
        "value": {"connector": "manzana", "api_key": "OLD"}
    }

    monkeypatch.setattr(
        module.BackendAPISecurity,
        "login_account",
        staticmethod(lambda **kwargs: True),
        raising=False,
    )

    def update_keys(account_name, config):
        events.append("file")
        file_state.update(exists=True, content="NEW")

    monkeypatch.setattr(
        module.BackendAPISecurity,
        "update_connector_keys",
        staticmethod(update_keys),
        raising=False,
    )
    monkeypatch.setattr(
        module.BackendAPISecurity,
        "decrypt_all",
        staticmethod(lambda **kwargs: events.append("decrypt")),
        raising=False,
    )
    monkeypatch.setattr(
        module.AllConnectorSettings,
        "get_connector_config_keys",
        staticmethod(lambda connector_name: settings_state["value"]),
        raising=False,
    )
    monkeypatch.setattr(
        module.AllConnectorSettings,
        "update_connector_config_keys",
        staticmethod(lambda value: settings_state.update(value=value)),
        raising=False,
    )

    class Config:
        pass

    monkeypatch.setattr(module, "HummingbotAPIConfigAdapter", lambda value: Config())
    monkeypatch.setattr(module, "api_keys_from_connector_config_map", lambda value: {})
    service._create_trading_connector = lambda *args, **kwargs: candidate

    async def initialize(*args):
        events.append("initialize")

    async def activate(connector, account_name, connector_name, staged_indices=None):
        events.append("activate")
        staged_indices.update(candidate_indices)

    service._initialize_trading_connector_candidate = initialize
    service._activate_trading_connector_candidate = activate
    monkeypatch.setattr(
        module.fs_util,
        "path_exists",
        lambda path: file_state["exists"],
        raising=False,
    )
    monkeypatch.setattr(
        module.fs_util,
        "read_file",
        lambda path: file_state["content"],
        raising=False,
    )
    monkeypatch.setattr(
        module.fs_util,
        "ensure_file_and_dump_text",
        lambda path, content: file_state.update(exists=True, content=content),
        raising=False,
    )

    detail = CapabilityErrorDetail(
        code="connector_cleanup_residual",
        message="prior stop failed",
        connector="manzana",
        feature="connector_cleanup",
        reason="cleanup_incomplete",
        cleanup_scope="connector",
        cleanup_code="connector_stop_returned_false",
    )

    async def stop_prior(account_name, connector_name):
        events.append("stop-prior")
        CHECK.assertEqual(
            ["initialize", "activate", "file", "decrypt"], events[:-1]
        )
        return detail

    async def stop_index(account_name, connector_name, owner_kind, value):
        events.append((owner_kind, value))

    async def stop_candidate(*args, **kwargs):
        events.append("stop-candidate")

    service._stop_trading_connector_already_locked = stop_prior
    service._stop_owned_index = stop_index
    service._stop_connector_network = stop_candidate

    async with service.credential_transaction("acct", "manzana"):
        stage = await service.stage_connector_keys_already_locked(
            "acct", "manzana", {"api_key": "NEW"}
        )
        with pytest.raises(ConnectorCapabilityError):
            await service.commit_staged_connector_keys_already_locked(
                "acct", "manzana", stage
            )
        await service.discard_staged_connector_already_locked(
            "acct", "manzana", stage
        )

    CHECK.assertEqual("OLD", file_state["content"])
    CHECK.assertEqual(
        {"connector": "manzana", "api_key": "OLD"},
        settings_state["value"],
    )
    CHECK.assertIs(old_connector, service._trading_connectors["acct"]["manzana"])
    CHECK.assertEqual(old_indices["orders"], service._orders_recorders[cache_key])
    CHECK.assertEqual(old_indices["funding"], service._funding_recorders[cache_key])
    CHECK.assertEqual(old_indices["metrics"], service._metrics_collectors[cache_key])
    for label, value in candidate_indices.items():
        CHECK.assertIn((f"credential_candidate_{label}", value), events)
    CHECK.assertIn("stop-candidate", events)


@pytest.mark.asyncio
async def test_issue224_credential_delete_file_failure_restores_before_stop(
    monkeypatch,
):
    from contextlib import asynccontextmanager

    accounts_module = _load_accounts_service_module()
    events = []
    file_state = {"exists": True, "content": "EXACT-OLD"}

    class ConnectorService:
        @asynccontextmanager
        async def credential_transaction(self, account_name, connector_name):
            events.append("enter")
            try:
                yield
            finally:
                events.append("exit")

        async def _stop_trading_connector_already_locked(self, *args):
            events.append("stop")
            return None

    service = object.__new__(accounts_module.AccountsService)
    service._connector_service = ConnectorService()
    service.accounts_state = {"acct": {"manzana": ["EXACT-STATE"]}}
    monkeypatch.setattr(accounts_module.fs_util, "path_exists", lambda path: True, raising=False)
    monkeypatch.setattr(
        accounts_module.fs_util,
        "read_file",
        lambda path: file_state["content"],
        raising=False,
    )

    def fail_after_delete(**kwargs):
        events.append("delete")
        file_state["exists"] = False
        raise PermissionError("DELETE-FAILED")

    monkeypatch.setattr(
        accounts_module.fs_util, "delete_file", fail_after_delete, raising=False
    )

    def restore(path, content):
        events.append("restore")
        file_state.update(exists=True, content=content)

    monkeypatch.setattr(
        accounts_module.fs_util,
        "ensure_file_and_dump_text",
        restore,
        raising=False,
    )

    with pytest.raises(PermissionError, match="DELETE-FAILED"):
        await service.delete_credentials("acct", "manzana")
    CHECK.assertEqual(["enter", "delete", "restore", "exit"], events)
    CHECK.assertEqual({"exists": True, "content": "EXACT-OLD"}, file_state)
    CHECK.assertEqual(["EXACT-STATE"], service.accounts_state["acct"]["manzana"])


@pytest.mark.asyncio
async def test_issue224_account_delete_excludes_successor_through_final_commit(
    monkeypatch,
):
    accounts_module = _load_accounts_service_module()
    module, registry_type = _load_unified_service_module()
    connector_service = _final_boundary_service(module, registry_type)
    connector_service._trading_connectors = {
        "acct": {"zeta": object(), "alpha": object()}
    }
    service = object.__new__(accounts_module.AccountsService)
    service._connector_service = connector_service
    service.accounts_state = {"acct": {"alpha": ["A"], "zeta": ["Z"]}}
    events = []
    first_stop_entered = asyncio.Event()
    release_first_stop = asyncio.Event()
    folder_state = {"exists": True}

    monkeypatch.setattr(
        accounts_module.fs_util,
        "snapshot_folder",
        lambda *args: {"directories": (".",), "files": {"key": b"secret"}},
        raising=False,
    )

    def delete_folder(*args):
        events.append("delete-folder")
        folder_state["exists"] = False

    monkeypatch.setattr(
        accounts_module.fs_util, "delete_folder", delete_folder, raising=False
    )

    async def controlled_stop(account_name, connector_name):
        events.append(f"stop-{connector_name}")
        if connector_name == "alpha":
            first_stop_entered.set()
            await release_first_stop.wait()
        return True

    connector_service.stop_trading_connector = controlled_stop
    delete_task = asyncio.create_task(service.delete_account("acct"))
    await first_stop_entered.wait()

    async def publish_successor():
        async with connector_service.credential_transaction("acct", "successor"):
            CHECK.assertNotIn("acct", service.accounts_state)
            CHECK.assertFalse(folder_state["exists"])
            events.append("publish-successor")

    successor = asyncio.create_task(publish_successor())
    await asyncio.sleep(0)
    CHECK.assertFalse(successor.done())
    release_first_stop.set()
    await delete_task
    await successor
    CHECK.assertEqual(
        [
            "delete-folder",
            "stop-alpha",
            "stop-zeta",
            "publish-successor",
        ],
        events,
    )
    CHECK.assertEqual({}, connector_service._account_transaction_locks)


@pytest.mark.asyncio
async def test_issue224_account_delete_filesystem_failure_restores_exact_snapshot(
    monkeypatch,
):
    from contextlib import asynccontextmanager

    accounts_module = _load_accounts_service_module()
    events = []
    account_state = {"manzana": [{"token": "USD", "units": 7}]}
    folder_state = {"files": {"connectors/manzana.yml": b"EXACT"}}
    snapshot = {
        "directories": (".", "connectors"),
        "files": dict(folder_state["files"]),
    }

    class ConnectorService:
        @asynccontextmanager
        async def account_transaction(self, account_name):
            events.append("enter")
            try:
                yield
            finally:
                events.append("exit")

        def list_account_connectors(self, account_name):
            return ["manzana"]

        async def stop_trading_connector(self, account_name, connector_name):
            events.append("stop")

        def clear_account_connectors_already_locked(self, account_name):
            events.append("clear")

    service = object.__new__(accounts_module.AccountsService)
    service._connector_service = ConnectorService()
    service.accounts_state = {"acct": copy.deepcopy(account_state)}
    monkeypatch.setattr(
        accounts_module.fs_util,
        "snapshot_folder",
        lambda *args: copy.deepcopy(snapshot),
        raising=False,
    )

    def fail_delete(*args):
        events.append("delete")
        folder_state["files"].clear()
        raise PermissionError("FOLDER-DELETE-FAILED")

    def restore(*args):
        events.append("restore")
        folder_state["files"] = dict(args[-1]["files"])

    monkeypatch.setattr(
        accounts_module.fs_util, "delete_folder", fail_delete, raising=False
    )
    monkeypatch.setattr(
        accounts_module.fs_util,
        "restore_folder_snapshot",
        restore,
        raising=False,
    )

    with pytest.raises(PermissionError, match="FOLDER-DELETE-FAILED"):
        await service.delete_account("acct")
    CHECK.assertEqual(["enter", "delete", "restore", "exit"], events)
    CHECK.assertEqual(snapshot["files"], folder_state["files"])
    CHECK.assertEqual(account_state, service.accounts_state["acct"])


@pytest.mark.asyncio
async def test_issue224_periodic_balance_failure_preserves_prior_state(monkeypatch):
    accounts_module = _load_accounts_service_module()
    good = object()
    stale = object()
    calls = []

    class ConnectorService:
        def get_all_trading_connectors(self):
            return {"acct": {"good": good, "stale": stale}}

        def is_gateway_connector(self, connector):
            return False

    service = object.__new__(accounts_module.AccountsService)
    service._connector_service = ConnectorService()
    service.accounts_state = {
        "acct": {"good": ["OLD-GOOD"], "stale": ["OLD-STALE"]}
    }
    service.update_account_state_interval = 1
    service.require_capability = lambda *args, **kwargs: None

    async def no_op():
        return None

    service.check_all_connectors = no_op
    service._update_gateway_balances = no_op
    service._mirror_gateway_state_to_accounts = lambda meta: None
    service.dump_account_state = no_op

    async def refresh(connector, connector_name, account_name):
        calls.append((connector, connector_name, account_name))
        if connector is stale:
            raise RuntimeError("STALE-INCARNATION")
        service.accounts_state[account_name][connector_name] = ["NEW-GOOD"]
        return ["NEW-GOOD"]

    service._refresh_account_connector_state = refresh

    async def stop_loop(delay):
        raise asyncio.CancelledError()

    monkeypatch.setattr(accounts_module.asyncio, "sleep", stop_loop)
    with pytest.raises(asyncio.CancelledError):
        await service.update_account_state_loop()
    CHECK.assertEqual(["NEW-GOOD"], service.accounts_state["acct"]["good"])
    CHECK.assertEqual(["OLD-STALE"], service.accounts_state["acct"]["stale"])
    CHECK.assertEqual(
        [(good, "good", "acct"), (stale, "stale", "acct")], calls
    )


@pytest.mark.asyncio
async def test_issue224_credential_update_cleanup_failure_precedes_terminal_stop(
    monkeypatch,
):
    accounts_module = _load_accounts_service_module()
    module, registry_type = _load_unified_service_module()
    connector_service = _final_boundary_service(module, registry_type)
    connector_key = ("acct", "manzana")
    cache_key = "acct:manzana"
    events = []
    file_state = {"exists": True, "content": "OLD"}

    class Connector:
        def __init__(self, label):
            self.label = label

        async def stop_network(self):
            events.append(f"stop-{self.label}")

    class FailingIndex:
        async def stop(self):
            events.append("index-fail")
            raise RuntimeError("INDEX-FAIL")

    old_connector = Connector("old")
    candidate = Connector("candidate")
    index = FailingIndex()
    connector_service._trading_connectors = {
        "acct": {"manzana": old_connector}
    }
    connector_service._orders_recorders[cache_key] = index
    connector_service._advance_connector_incarnation(connector_key)

    async def stage(*args, **kwargs):
        file_state["content"] = "NEW"
        return {
            "connector": candidate,
            "indices": {},
            "committed": False,
            "discarded": False,
            "credential_snapshot": {
                "path": "credentials/acct/connectors/manzana.yml",
                "existed": True,
                "content": "OLD",
                "security": {
                    "secure_configs": None,
                    "decryption_done": None,
                },
                "connector_settings": None,
            },
            "credentials_touched": True,
            "credentials_committed": False,
            "credentials_rolled_back": False,
        }

    connector_service.stage_connector_keys_already_locked = stage
    service = object.__new__(accounts_module.AccountsService)
    service._connector_service = connector_service
    service.accounts_state = {"acct": {"manzana": ["OLD-STATE"]}}
    service._get_connector_tokens_info = lambda *args, **kwargs: asyncio.sleep(
        0, result=["NEW-STATE"]
    )

    monkeypatch.setattr(
        accounts_module.fs_util,
        "path_exists",
        lambda path: file_state["exists"],
        raising=False,
    )
    monkeypatch.setattr(
        accounts_module.fs_util,
        "read_file",
        lambda path: file_state["content"],
        raising=False,
    )

    def restore_file(path, content):
        events.append("restore-file")
        file_state.update(exists=True, content=content)

    monkeypatch.setattr(
        accounts_module.fs_util,
        "ensure_file_and_dump_text",
        restore_file,
        raising=False,
    )

    with pytest.raises(module.ConnectorCapabilityError) as failure:
        await service.add_credentials(
            "acct", "manzana", {"api_key": "NEW"}
        )

    CHECK.assertEqual(
        "orders_cleanup_incomplete", failure.value.detail["cleanup_code"]
    )
    CHECK.assertEqual(
        ["index-fail", "stop-candidate", "restore-file"],
        events,
    )
    CHECK.assertIs(
        old_connector,
        connector_service._trading_connectors["acct"]["manzana"],
    )
    CHECK.assertIs(index, connector_service._orders_recorders[cache_key])
    CHECK.assertEqual("OLD", file_state["content"])
    CHECK.assertEqual(
        ["OLD-STATE"], service.accounts_state["acct"]["manzana"]
    )


@pytest.mark.asyncio
async def test_issue224_credential_delete_cleanup_failure_precedes_terminal_stop(
    monkeypatch,
):
    accounts_module = _load_accounts_service_module()
    module, registry_type = _load_unified_service_module()
    connector_service = _final_boundary_service(module, registry_type)
    connector_key = ("acct", "manzana")
    cache_key = "acct:manzana"
    events = []
    file_state = {"exists": True, "content": "OLD"}

    class Connector:
        async def stop_network(self):
            events.append("stop-old")

    class FailingIndex:
        async def stop(self):
            events.append("index-fail")
            raise RuntimeError("INDEX-FAIL")

    old_connector = Connector()
    index = FailingIndex()
    connector_service._trading_connectors = {
        "acct": {"manzana": old_connector}
    }
    connector_service._orders_recorders[cache_key] = index
    connector_service._advance_connector_incarnation(connector_key)
    service = object.__new__(accounts_module.AccountsService)
    service._connector_service = connector_service
    service.accounts_state = {"acct": {"manzana": ["OLD-STATE"]}}

    monkeypatch.setattr(
        accounts_module.fs_util,
        "path_exists",
        lambda path: file_state["exists"],
        raising=False,
    )
    monkeypatch.setattr(
        accounts_module.fs_util,
        "read_file",
        lambda path: file_state["content"],
        raising=False,
    )

    def delete_file(**kwargs):
        events.append("delete-file")
        file_state["exists"] = False

    def restore_file(path, content):
        events.append("restore-file")
        file_state.update(exists=True, content=content)

    monkeypatch.setattr(
        accounts_module.fs_util, "delete_file", delete_file, raising=False
    )
    monkeypatch.setattr(
        accounts_module.fs_util,
        "ensure_file_and_dump_text",
        restore_file,
        raising=False,
    )

    with pytest.raises(module.ConnectorCapabilityError) as failure:
        await service.delete_credentials("acct", "manzana")

    CHECK.assertEqual(
        "orders_cleanup_incomplete", failure.value.detail["cleanup_code"]
    )
    CHECK.assertEqual(
        ["delete-file", "index-fail", "restore-file"], events
    )
    CHECK.assertIs(
        old_connector,
        connector_service._trading_connectors["acct"]["manzana"],
    )
    CHECK.assertIs(index, connector_service._orders_recorders[cache_key])
    CHECK.assertEqual({"exists": True, "content": "OLD"}, file_state)
    CHECK.assertEqual(
        ["OLD-STATE"], service.accounts_state["acct"]["manzana"]
    )


@pytest.mark.asyncio
async def test_issue224_account_delete_rolls_forward_after_prior_terminal_stop(
    monkeypatch,
):
    from contextlib import asynccontextmanager

    accounts_module = _load_accounts_service_module()
    module, _ = _load_unified_service_module()
    detail = module.CapabilityErrorDetail(
        code="connector_cleanup_residual",
        message="zeta cleanup failed",
        connector="zeta",
        feature="connector_cleanup",
        reason="cleanup_incomplete",
        cleanup_scope="connector",
        cleanup_code="orders_cleanup_incomplete",
    )
    live_connectors = {"alpha", "zeta"}
    events = []
    folder_state = {
        "directories": (".", "connectors"),
        "files": {
            "conf_client.yml": b"CONFIG",
            "connectors/alpha.yml": b"ALPHA",
            "connectors/zeta.yml": b"ZETA",
        },
    }
    snapshot = copy.deepcopy(folder_state)

    class ConnectorService:
        @asynccontextmanager
        async def account_transaction(self, account_name):
            yield

        def list_account_connectors(self, account_name):
            return sorted(live_connectors)

        async def stop_trading_connector(self, account_name, connector_name):
            events.append(f"stop-{connector_name}")
            if connector_name == "alpha":
                live_connectors.remove(connector_name)
                return True
            raise module.ConnectorCapabilityError(detail)

        def clear_account_connectors_already_locked(self, account_name):
            events.append("clear")

    service = object.__new__(accounts_module.AccountsService)
    service._connector_service = ConnectorService()
    service.accounts_state = {
        "acct": {"alpha": ["A"], "zeta": ["Z"]}
    }
    monkeypatch.setattr(
        accounts_module.fs_util,
        "snapshot_folder",
        lambda *args: copy.deepcopy(snapshot),
        raising=False,
    )

    def delete_folder(*args):
        events.append("delete-folder")
        folder_state["directories"] = ()
        folder_state["files"] = {}

    def restore_folder(*args):
        events.append("restore-folder")
        restored = args[-1]
        folder_state["directories"] = restored["directories"]
        folder_state["files"] = dict(restored["files"])

    monkeypatch.setattr(
        accounts_module.fs_util,
        "delete_folder",
        delete_folder,
        raising=False,
    )
    monkeypatch.setattr(
        accounts_module.fs_util,
        "restore_folder_snapshot",
        restore_folder,
        raising=False,
    )

    with pytest.raises(module.ConnectorCapabilityError):
        await service.delete_account("acct")

    CHECK.assertEqual(
        ["delete-folder", "stop-alpha", "stop-zeta", "restore-folder"],
        events,
    )
    CHECK.assertEqual({"zeta"}, live_connectors)
    CHECK.assertEqual({"zeta": ["Z"]}, service.accounts_state["acct"])
    CHECK.assertEqual(
        {
            "conf_client.yml": b"CONFIG",
            "connectors/zeta.yml": b"ZETA",
        },
        folder_state["files"],
    )


@pytest.mark.asyncio
async def test_issue224_account_registry_pruning_ignores_repeated_cancellation():
    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)
    entered = asyncio.Event()
    leave = asyncio.Event()

    async def worker():
        async with service.account_transaction("acct"):
            entered.set()
            await leave.wait()

    task = asyncio.create_task(worker())
    await entered.wait()
    await service._account_transaction_locks_guard.acquire()
    leave.set()
    try:
        for _ in range(8):
            task.cancel()
            await asyncio.sleep(0)
        await asyncio.gather(task, return_exceptions=True)
    finally:
        service._account_transaction_locks_guard.release()
    await asyncio.sleep(0)

    CHECK.assertEqual({}, service._account_transaction_lock_users)
    CHECK.assertEqual({}, service._account_transaction_locks)
    CHECK.assertEqual({}, service._account_transaction_owners)


@pytest.mark.asyncio
async def test_issue224_pre_stop_cancellation_rolls_back_every_stopped_index():
    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)
    connector = Issue224RepairConnector()
    connector_key = ("acct", "manzana")
    cache_key = "acct:manzana"
    metrics_entered = asyncio.Event()

    class Index:
        def __init__(self, label, block=False):
            self.label = label
            self.block = block
            self.stop_calls = 0
            self.start_calls = 0

        async def stop(self):
            self.stop_calls += 1
            if self.block:
                metrics_entered.set()
                await asyncio.Event().wait()

        async def start(self, *args):
            self.start_calls += 1

    orders = Index("orders")
    funding = Index("funding")
    metrics = Index("metrics", block=True)
    service._trading_connectors = {"acct": {"manzana": connector}}
    service._orders_recorders[cache_key] = orders
    service._funding_recorders[cache_key] = funding
    service._metrics_collectors[cache_key] = metrics
    service._advance_connector_incarnation(connector_key)

    operation = asyncio.create_task(
        service._finalize_stop_trading_connector("acct", "manzana")
    )
    await metrics_entered.wait()
    operation.cancel("exact-pre-stop-cancellation")
    with pytest.raises(asyncio.CancelledError) as captured:
        await operation

    CHECK.assertEqual(("exact-pre-stop-cancellation",), captured.value.args)
    CHECK.assertEqual(1, orders.stop_calls)
    CHECK.assertEqual(1, funding.stop_calls)
    CHECK.assertEqual(1, metrics.stop_calls)
    CHECK.assertEqual(1, orders.start_calls)
    CHECK.assertEqual(1, funding.start_calls)
    CHECK.assertEqual(1, metrics.start_calls)
    CHECK.assertEqual(0, connector.stop_calls)
    CHECK.assertIs(connector, service._trading_connectors["acct"]["manzana"])


@pytest.mark.asyncio
async def test_issue224_pre_stop_resume_attempts_every_index_after_failure():
    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)
    connector = Issue224RepairConnector()
    service._trading_connectors = {"acct": {"manzana": connector}}
    events = []

    class Index:
        def __init__(self, label, fail=False):
            self.label = label
            self.fail = fail

        async def start(self, *args):
            events.append(self.label)
            if self.fail:
                raise RuntimeError(f"{self.label}-resume-failed")

    rollback_code = await service._resume_pre_stop_indices(
        ("acct", "manzana"),
        [
            ("orders", Index("orders")),
            ("funding", Index("funding", fail=True)),
            ("metrics", Index("metrics")),
        ],
    )

    CHECK.assertEqual(["metrics", "funding", "orders"], events)
    CHECK.assertEqual("funding_rollback_incomplete", rollback_code)


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal_outcome", ["false", "raise"])
async def test_issue224_terminal_network_failure_keeps_publication_and_resumes(
    terminal_outcome,
):
    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)
    connector_key = ("acct", "manzana")
    cache_key = "acct:manzana"

    class Connector(Issue224RepairConnector):
        async def stop_network(self):
            self.stop_calls += 1
            if terminal_outcome == "false":
                return False
            raise RuntimeError("terminal-network-failure")

    class Index:
        def __init__(self):
            self.stop_calls = 0
            self.start_calls = 0

        async def stop(self):
            self.stop_calls += 1

        async def start(self, connector):
            self.start_calls += 1

    connector = Connector()
    orders = Index()
    service._trading_connectors = {"acct": {"manzana": connector}}
    service._orders_recorders[cache_key] = orders
    service._advance_connector_incarnation(connector_key)

    for _ in range(2):
        with pytest.raises(module.ConnectorCapabilityError):
            await service.stop_trading_connector("acct", "manzana")

    CHECK.assertIs(connector, service._trading_connectors["acct"]["manzana"])
    CHECK.assertIs(orders, service._orders_recorders[cache_key])
    CHECK.assertEqual(1, orders.stop_calls)
    CHECK.assertEqual(1, orders.start_calls)
    CHECK.assertEqual(1, connector.stop_calls)


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal_outcome", ["success", "false", "raise"])
async def test_issue224_pending_network_terminal_reconciles_pre_stop_snapshot_once(
    terminal_outcome,
):
    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)
    connector_key = ("acct", "manzana")
    cache_key = "acct:manzana"
    entered = asyncio.Event()
    release = asyncio.Event()

    class Connector(Issue224RepairConnector):
        async def stop_network(self):
            self.stop_calls += 1
            entered.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await release.wait()
            if terminal_outcome == "false":
                return False
            if terminal_outcome == "raise":
                raise RuntimeError("terminal-network-failure")
            return True

    class Index:
        def __init__(self):
            self.stop_calls = 0
            self.start_calls = 0

        async def stop(self):
            self.stop_calls += 1

        async def start(self, connector):
            self.start_calls += 1

    connector = Connector()
    orders = Index()
    service._trading_connectors = {"acct": {"manzana": connector}}
    service._orders_recorders[cache_key] = orders
    service._advance_connector_incarnation(connector_key)

    with pytest.raises(module.ConnectorCapabilityError):
        await service.stop_trading_connector("acct", "manzana")
    await entered.wait()
    CHECK.assertEqual(1, orders.stop_calls)
    CHECK.assertEqual(0, orders.start_calls)
    CHECK.assertIs(connector, service._trading_connectors["acct"]["manzana"])

    retry_detail = await service._finalize_stop_trading_connector(
        "acct", "manzana"
    )
    CHECK.assertIsNotNone(retry_detail)
    CHECK.assertEqual(1, orders.stop_calls)

    release.set()
    for _ in range(200):
        if not service._market_locks:
            break
        await asyncio.sleep(0.001)

    if terminal_outcome == "success":
        CHECK.assertNotIn("acct", service._trading_connectors)
        CHECK.assertNotIn(cache_key, service._orders_recorders)
        CHECK.assertEqual(0, orders.start_calls)
        CHECK.assertNotIn(connector_key, service._connector_cleanup_indices)
    else:
        CHECK.assertIs(connector, service._trading_connectors["acct"]["manzana"])
        CHECK.assertIs(orders, service._orders_recorders[cache_key])
        CHECK.assertEqual(1, orders.start_calls)
    CHECK.assertEqual(1, orders.stop_calls)
    CHECK.assertEqual(1, connector.stop_calls)


def test_issue224_internal_account_names_are_typed_validation_errors():
    accounts_module = _load_accounts_service_module()

    for account_name in ("__data__", "__unscoped__", "__future_internal__"):
        with pytest.raises(accounts_module.ConnectorCapabilityError) as failure:
            accounts_module.validate_safe_name(account_name, "account name")
        CHECK.assertEqual(
            "invalid_account_resource_name", failure.value.detail["code"]
        )
        CHECK.assertEqual("reserved_name", failure.value.detail["reason"])


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal_outcome", ["success", "false"])
async def test_issue224_credential_delete_cancellation_has_terminal_guardian(
    monkeypatch,
    terminal_outcome,
):
    accounts_module = _load_accounts_service_module()
    module, registry_type = _load_unified_service_module()
    connector_service = _final_boundary_service(module, registry_type)
    connector_key = ("acct", "manzana")
    entered = asyncio.Event()
    release = asyncio.Event()
    file_state = {"exists": True, "content": "OLD"}

    class Connector(Issue224RepairConnector):
        async def stop_network(self):
            entered.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await release.wait()
            return terminal_outcome == "success"

    connector = Connector()
    connector_service._trading_connectors = {"acct": {"manzana": connector}}
    connector_service._advance_connector_incarnation(connector_key)
    service = object.__new__(accounts_module.AccountsService)
    service._connector_service = connector_service
    service.accounts_state = {"acct": {"manzana": ["OLD-STATE"]}}

    monkeypatch.setattr(
        accounts_module.fs_util,
        "path_exists",
        lambda path: file_state["exists"],
        raising=False,
    )
    monkeypatch.setattr(
        accounts_module.fs_util,
        "read_file",
        lambda path: file_state["content"],
        raising=False,
    )
    monkeypatch.setattr(
        accounts_module.fs_util,
        "delete_file",
        lambda **kwargs: file_state.update(exists=False),
        raising=False,
    )
    monkeypatch.setattr(
        accounts_module.fs_util,
        "ensure_file_and_dump_text",
        lambda path, content: file_state.update(exists=True, content=content),
        raising=False,
    )

    caller = asyncio.create_task(service.delete_credentials("acct", "manzana"))
    await entered.wait()
    caller.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(caller, 0.2)

    CHECK.assertFalse(file_state["exists"])
    release.set()
    for _ in range(200):
        connector_live = connector_service._trading_connectors.get("acct", {}).get(
            "manzana"
        )
        terminal = (
            connector_live is None
            if terminal_outcome == "success"
            else file_state["exists"]
        )
        if terminal:
            break
        await asyncio.sleep(0.001)
    for _ in range(1000):
        if not service._account_continuations:
            break
        await asyncio.sleep(0.001)
    CHECK.assertEqual(set(), service._account_continuations)

    if terminal_outcome == "success":
        CHECK.assertFalse(file_state["exists"])
        CHECK.assertNotIn("manzana", service.accounts_state.get("acct", {}))
        CHECK.assertNotIn("acct", connector_service._trading_connectors)
    else:
        CHECK.assertEqual({"exists": True, "content": "OLD"}, file_state)
        CHECK.assertEqual(
            ["OLD-STATE"], service.accounts_state["acct"]["manzana"]
        )
        CHECK.assertIs(
            connector,
            connector_service._trading_connectors["acct"]["manzana"],
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal_outcome", ["success", "false"])
async def test_issue224_credential_update_cancellation_has_terminal_guardian(
    monkeypatch,
    terminal_outcome,
):
    accounts_module = _load_accounts_service_module()
    module, registry_type = _load_unified_service_module()
    connector_service = _final_boundary_service(module, registry_type)
    connector_key = ("acct", "manzana")
    entered = asyncio.Event()
    release = asyncio.Event()
    file_state = {"exists": True, "content": "OLD"}

    class Connector(Issue224RepairConnector):
        def __init__(self, retained=False):
            super().__init__()
            self.retained = retained

        async def stop_network(self):
            self.stop_calls += 1
            if not self.retained:
                return True
            entered.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await release.wait()
            return terminal_outcome == "success"

    old_connector = Connector(retained=True)
    candidate = Connector()
    connector_service._trading_connectors = {
        "acct": {"manzana": old_connector}
    }
    connector_service._advance_connector_incarnation(connector_key)

    async def stage(*args, **kwargs):
        file_state["content"] = "NEW"
        return {
            "connector": candidate,
            "indices": {},
            "committed": False,
            "discarded": False,
            "credential_snapshot": {
                "path": "credentials/acct/connectors/manzana.yml",
                "existed": True,
                "content": "OLD",
                "security": {
                    "secure_configs": None,
                    "decryption_done": None,
                },
                "connector_settings": None,
            },
            "credentials_touched": True,
            "credentials_committed": False,
            "credentials_rolled_back": False,
        }

    connector_service.stage_connector_keys_already_locked = stage
    service = object.__new__(accounts_module.AccountsService)
    service._connector_service = connector_service
    service.accounts_state = {"acct": {"manzana": ["OLD-STATE"]}}
    service._get_connector_tokens_info = lambda *args, **kwargs: asyncio.sleep(
        0, result=["NEW-STATE"]
    )
    monkeypatch.setattr(
        accounts_module.fs_util,
        "path_exists",
        lambda path: file_state["exists"],
        raising=False,
    )
    monkeypatch.setattr(
        accounts_module.fs_util,
        "read_file",
        lambda path: file_state["content"],
        raising=False,
    )
    monkeypatch.setattr(
        accounts_module.fs_util,
        "ensure_file_and_dump_text",
        lambda path, content: file_state.update(exists=True, content=content),
        raising=False,
    )

    caller = asyncio.create_task(
        service.add_credentials("acct", "manzana", {"api_key": "NEW"})
    )
    await entered.wait()
    caller.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(caller, 0.2)

    CHECK.assertEqual("NEW", file_state["content"])
    release.set()
    for _ in range(200):
        published = connector_service._trading_connectors.get("acct", {}).get(
            "manzana"
        )
        terminal = (
            published is candidate
            if terminal_outcome == "success"
            else file_state["content"] == "OLD"
        )
        if terminal:
            break
        await asyncio.sleep(0.001)
    for _ in range(1000):
        if not service._account_continuations:
            break
        await asyncio.sleep(0.001)
    CHECK.assertEqual(set(), service._account_continuations)

    if terminal_outcome == "success":
        CHECK.assertEqual("NEW", file_state["content"])
        CHECK.assertEqual(
            ["NEW-STATE"], service.accounts_state["acct"]["manzana"]
        )
        CHECK.assertIs(
            candidate,
            connector_service._trading_connectors["acct"]["manzana"],
        )
    else:
        CHECK.assertEqual("OLD", file_state["content"])
        CHECK.assertEqual(
            ["OLD-STATE"], service.accounts_state["acct"]["manzana"]
        )
        CHECK.assertIs(
            old_connector,
            connector_service._trading_connectors["acct"]["manzana"],
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal_outcome", ["success", "false"])
async def test_issue224_account_delete_cancellation_has_terminal_guardian(
    monkeypatch,
    terminal_outcome,
):
    accounts_module = _load_accounts_service_module()
    module, registry_type = _load_unified_service_module()
    connector_service = _final_boundary_service(module, registry_type)
    connector_key = ("acct", "manzana")
    entered = asyncio.Event()
    release = asyncio.Event()
    folder_state = {
        "directories": (".", "connectors"),
        "files": {"connectors/manzana.yml": b"OLD"},
    }
    folder_snapshot = copy.deepcopy(folder_state)

    class Connector(Issue224RepairConnector):
        async def stop_network(self):
            entered.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await release.wait()
            return terminal_outcome == "success"

    connector = Connector()
    connector_service._trading_connectors = {"acct": {"manzana": connector}}
    connector_service._advance_connector_incarnation(connector_key)
    service = object.__new__(accounts_module.AccountsService)
    service._connector_service = connector_service
    service.accounts_state = {"acct": {"manzana": ["OLD-STATE"]}}

    monkeypatch.setattr(
        accounts_module.fs_util,
        "snapshot_folder",
        lambda *args: copy.deepcopy(folder_snapshot),
        raising=False,
    )
    monkeypatch.setattr(
        accounts_module.fs_util,
        "delete_folder",
        lambda *args: folder_state.update(directories=(), files={}),
        raising=False,
    )

    def restore_folder(*args):
        snapshot = args[-1]
        folder_state["directories"] = snapshot["directories"]
        folder_state["files"] = dict(snapshot["files"])

    monkeypatch.setattr(
        accounts_module.fs_util,
        "restore_folder_snapshot",
        restore_folder,
        raising=False,
    )

    caller = asyncio.create_task(service.delete_account("acct"))
    await entered.wait()
    caller.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(caller, 0.2)

    CHECK.assertEqual({}, folder_state["files"])
    release.set()
    for _ in range(200):
        terminal = (
            "acct" not in service.accounts_state
            if terminal_outcome == "success"
            else bool(folder_state["files"])
        )
        if terminal:
            break
        await asyncio.sleep(0.001)
    for _ in range(1000):
        if not service._account_continuations:
            break
        await asyncio.sleep(0.001)
    CHECK.assertEqual(set(), service._account_continuations)

    if terminal_outcome == "success":
        CHECK.assertEqual({}, folder_state["files"])
        CHECK.assertNotIn("acct", service.accounts_state)
        CHECK.assertNotIn("acct", connector_service._trading_connectors)
    else:
        CHECK.assertEqual(folder_snapshot, folder_state)
        CHECK.assertEqual(
            ["OLD-STATE"], service.accounts_state["acct"]["manzana"]
        )
        CHECK.assertIs(
            connector,
            connector_service._trading_connectors["acct"]["manzana"],
        )


@pytest.mark.asyncio
async def test_issue224_current_pre_stop_index_is_reconciled_after_cancellation():
    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)
    connector = Issue224RepairConnector()
    connector_key = ("acct", "manzana")
    cache_key = "acct:manzana"
    stop_entered = asyncio.Event()
    release_stop = asyncio.Event()

    class Index:
        def __init__(self):
            self.stopped = False
            self.stop_calls = 0
            self.start_calls = 0

        async def stop(self):
            self.stop_calls += 1
            self.stopped = True
            stop_entered.set()
            await release_stop.wait()

        async def start(self):
            self.start_calls += 1
            self.stopped = False

    metrics = Index()
    service._trading_connectors = {"acct": {"manzana": connector}}
    service._metrics_collectors[cache_key] = metrics
    service._advance_connector_incarnation(connector_key)

    operation = asyncio.create_task(
        service._finalize_stop_trading_connector("acct", "manzana")
    )
    await stop_entered.wait()
    operation.cancel("current-index-cancel")
    with pytest.raises(asyncio.CancelledError) as captured:
        await asyncio.wait_for(operation, 0.2)
    CHECK.assertEqual(("current-index-cancel",), captured.value.args)

    release_stop.set()
    for _ in range(1000):
        if metrics.start_calls == 1:
            break
        await asyncio.sleep(0.001)

    CHECK.assertEqual(1, metrics.stop_calls)
    CHECK.assertEqual(1, metrics.start_calls)
    CHECK.assertFalse(metrics.stopped)
    CHECK.assertEqual(0, connector.stop_calls)
    CHECK.assertIs(metrics, service._metrics_collectors[cache_key])
    CHECK.assertIs(connector, service._trading_connectors["acct"]["manzana"])


@pytest.mark.asyncio
async def test_issue224_resume_retry_only_replays_the_unresolved_subset():
    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)
    connector_key = ("acct", "manzana")
    cache_key = "acct:manzana"

    class Connector(Issue224RepairConnector):
        async def stop_network(self):
            self.stop_calls += 1
            return False

    class Index:
        def __init__(self, fail_once=False):
            self.fail_once = fail_once
            self.stop_calls = 0
            self.start_calls = 0

        async def stop(self):
            self.stop_calls += 1

        async def start(self, connector):
            self.start_calls += 1
            if self.fail_once and self.start_calls == 1:
                raise RuntimeError("one-shot-resume-failure")

    connector = Connector()
    orders = Index()
    funding = Index(fail_once=True)
    service._trading_connectors = {"acct": {"manzana": connector}}
    service._orders_recorders[cache_key] = orders
    service._funding_recorders[cache_key] = funding
    service._advance_connector_incarnation(connector_key)

    for _ in range(2):
        with pytest.raises(module.ConnectorCapabilityError):
            await service.stop_trading_connector("acct", "manzana")

    owner_key = ("acct", "manzana", "network")
    phase = service._connector_cleanup_phases[owner_key]["connector_stop"]
    CHECK.assertEqual(1, connector.stop_calls)
    CHECK.assertEqual(1, orders.stop_calls)
    CHECK.assertEqual(1, funding.stop_calls)
    CHECK.assertEqual(1, orders.start_calls)
    CHECK.assertEqual(2, funding.start_calls)
    CHECK.assertEqual((), tuple(phase["unresolved_resume_indices"]))
    CHECK.assertTrue(phase["pre_stop_reconciled"])


@pytest.mark.asyncio
async def test_issue224_first_credential_cancellation_rolls_back_uncommitted_stage():
    from contextlib import asynccontextmanager

    accounts_module = _load_accounts_service_module()
    token_read_entered = asyncio.Event()
    candidate = object()
    publication = {
        "candidate": None,
        "file": None,
        "security": {"baseline": "OLD"},
    }
    calls = []

    class ConnectorService:
        @asynccontextmanager
        async def credential_transaction(self, account_name, connector_name):
            yield

        async def stage_connector_keys_already_locked(self, *args):
            publication["file"] = "NEW"
            publication["security"] = {"manzana": "NEW"}
            return {"connector": candidate, "committed": False}

        async def await_retained_connector_stop_already_locked(self, *args):
            calls.append("await-retained-stop")
            return None

        def is_trading_connector_initialized(self, *args):
            return False

        async def commit_staged_connector_keys_already_locked(self, *args):
            calls.append("commit")
            publication["candidate"] = candidate

        async def discard_staged_connector_already_locked(self, *args):
            calls.append("discard")
            publication["candidate"] = None
            publication["file"] = None
            publication["security"] = {"baseline": "OLD"}

    service = object.__new__(accounts_module.AccountsService)
    service._connector_service = ConnectorService()
    service.accounts_state = {}

    async def blocked_token_read(*args, **kwargs):
        token_read_entered.set()
        await asyncio.Event().wait()

    service._get_connector_tokens_info = blocked_token_read
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(
        accounts_module.fs_util, "path_exists", lambda path: False, raising=False
    )
    monkeypatch.setattr(
        accounts_module.fs_util,
        "delete_file",
        lambda **kwargs: publication.update(file=None),
        raising=False,
    )
    try:
        caller = asyncio.create_task(
            service.add_credentials("acct", "manzana", {"api_key": "NEW"})
        )
        await token_read_entered.wait()
        caller.cancel("first-credential-cancel")
        with pytest.raises(asyncio.CancelledError) as captured:
            await asyncio.wait_for(caller, 0.2)
        CHECK.assertEqual(("first-credential-cancel",), captured.value.args)

        for _ in range(1000):
            if not service._account_continuations:
                break
            await asyncio.sleep(0.001)
    finally:
        monkeypatch.undo()

    CHECK.assertEqual(set(), service._account_continuations)
    CHECK.assertEqual(["discard"], calls)
    CHECK.assertEqual(
        {"candidate": None, "file": None, "security": {"baseline": "OLD"}},
        publication,
    )
    CHECK.assertEqual({}, service.accounts_state)


def test_issue224_reserved_accounts_fail_at_model_router_service_and_unified_boundaries():
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from models.market_data import AddTradingPairRequest, RemoveTradingPairRequest
    from routers import market_data as market_data_router
    from services.connector_capabilities import ConnectorCapabilityError
    from services.market_data_service import MarketDataService

    for model in (AddTradingPairRequest, RemoveTradingPairRequest):
        with pytest.raises(ConnectorCapabilityError):
            model(
                connector_name="manzana",
                trading_pair="BTC-USD",
                account_name="__data__",
            )

    class GuardedMarketData:
        def require_capability(self, *args, **kwargs):
            raise AssertionError("reserved account reached connector capability work")

    app = FastAPI()
    app.include_router(market_data_router.router)
    app.dependency_overrides[market_data_router.get_market_data_service] = (
        GuardedMarketData
    )
    client = TestClient(app)
    responses = (
        client.post(
            "/market-data/trading-pair/add",
            json={
                "connector_name": "manzana",
                "trading_pair": "BTC-USD",
                "account_name": "__data__",
            },
        ),
        client.get(
            "/market-data/order-book/diagnostics/manzana",
            params={"account_name": "__data__"},
        ),
        client.post(
            "/market-data/order-book/restart/manzana",
            params={"account_name": "__data__"},
        ),
    )
    for response in responses:
        CHECK.assertEqual(400, response.status_code)
        CHECK.assertEqual(
            "invalid_account_resource_name", response.json()["detail"]["code"]
        )

    class GuardedConnectorService:
        def require_capability(self, *args, **kwargs):
            return None

        def get_best_connector_for_market(self, *args, **kwargs):
            raise AssertionError("reserved account reached connector resolution")

    market_data = MarketDataService(GuardedConnectorService())
    with pytest.raises(ConnectorCapabilityError):
        market_data.get_order_book("manzana", "BTC-USD", "__data__")

    module, registry_type = _load_unified_service_module()
    unified = _final_boundary_service(module, registry_type)
    unified._data_connectors["manzana"] = object()
    with pytest.raises(ConnectorCapabilityError):
        unified.get_best_connector_for_market("manzana", "__data__")
    with pytest.raises(ConnectorCapabilityError):
        unified._resolve_public_connector_key("manzana", "__future_internal__")


@pytest.mark.asyncio
async def test_issue224_security_snapshot_rollback_serializes_cross_account_commit():
    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)
    security = module.BackendAPISecurity
    security._secure_configs = {"baseline": "OLD"}
    security._decryption_done = asyncio.Event()
    security._decryption_done.set()
    first_entered = asyncio.Event()
    release_first = asyncio.Event()
    second_entered = asyncio.Event()

    async def rollback_first():
        async with service.credential_transaction("account-a", "manzana"):
            snapshot = service._security_state_snapshot()
            security._secure_configs["account-a"] = "UNCOMMITTED"
            first_entered.set()
            await release_first.wait()
            service._restore_staged_credential_publication(
                "account-a",
                "manzana",
                {
                    "credential_snapshot": {
                        "path": "credentials/account-a/connectors/manzana.yml",
                        "existed": False,
                        "content": None,
                        "security": snapshot,
                        "connector_settings": None,
                    },
                    "credentials_touched": True,
                    "credentials_committed": False,
                    "credentials_rolled_back": False,
                },
            )

    async def commit_second():
        async with service.credential_transaction("account-b", "manzana"):
            second_entered.set()
            security._secure_configs["account-b"] = "COMMITTED"

    first = asyncio.create_task(rollback_first())
    await first_entered.wait()
    second = asyncio.create_task(commit_second())
    await asyncio.sleep(0.01)
    second_was_blocked = not second_entered.is_set()
    release_first.set()
    await asyncio.gather(first, second)

    CHECK.assertTrue(second_was_blocked)
    CHECK.assertEqual(
        {"baseline": "OLD", "account-b": "COMMITTED"},
        security._secure_configs,
    )


@pytest.mark.asyncio
async def test_issue224_security_snapshot_precedes_production_login_mutation(
    monkeypatch,
):
    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)
    service.secrets_manager = object()
    security = module.BackendAPISecurity
    baseline = {"account-b": "COMMITTED"}
    monkeypatch.setattr(security, "_secure_configs", dict(baseline), raising=False)
    decryption_done = asyncio.Event()
    decryption_done.set()
    monkeypatch.setattr(
        security, "_decryption_done", decryption_done, raising=False
    )
    monkeypatch.setattr(
        module.AllConnectorSettings,
        "get_connector_config_keys",
        staticmethod(lambda connector_name: {"connector": connector_name}),
        raising=False,
    )
    monkeypatch.setattr(
        module.AllConnectorSettings,
        "update_connector_config_keys",
        staticmethod(lambda value: None),
        raising=False,
    )
    events = []
    captured = {}
    original_snapshot = service._credential_publication_snapshot
    original_discard = service.discard_staged_connector_already_locked

    def ordered_snapshot(account_name, connector_name):
        events.append("snapshot")
        return original_snapshot(account_name, connector_name)

    def failing_login(**kwargs):
        events.append("login")
        security._secure_configs.clear()
        security._secure_configs["account-a"] = "LOGIN-PARTIAL"
        decryption_done.clear()
        raise RuntimeError("login-failed-after-map-mutation")

    async def observed_discard(account_name, connector_name, stage):
        captured.update(stage)
        await original_discard(account_name, connector_name, stage)
        captured.clear()
        captured.update(stage)

    service._credential_publication_snapshot = ordered_snapshot
    service.discard_staged_connector_already_locked = observed_discard
    monkeypatch.setattr(
        security,
        "login_account",
        staticmethod(failing_login),
        raising=False,
    )

    async with service.credential_transaction("account-a", "manzana"):
        with pytest.raises(
            RuntimeError, match="login-failed-after-map-mutation"
        ):
            await service.stage_connector_keys_already_locked(
                "account-a", "manzana", {"api_key": "NEW"}
            )

    CHECK.assertEqual(["snapshot", "login"], events)
    CHECK.assertIn("credential_snapshot", captured)
    CHECK.assertTrue(captured["credentials_touched"])
    CHECK.assertTrue(captured["credentials_rolled_back"])
    CHECK.assertEqual(baseline, security._secure_configs)
    CHECK.assertTrue(decryption_done.is_set())


@pytest.mark.asyncio
async def test_issue224_staging_cancellation_restores_prelogin_global_map(
    monkeypatch,
):
    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)
    service.secrets_manager = object()
    security = module.BackendAPISecurity
    baseline = {"account-b": "COMMITTED"}
    monkeypatch.setattr(security, "_secure_configs", dict(baseline), raising=False)
    decryption_done = asyncio.Event()
    decryption_done.set()
    monkeypatch.setattr(
        security, "_decryption_done", decryption_done, raising=False
    )
    monkeypatch.setattr(
        module.AllConnectorSettings,
        "get_connector_config_keys",
        staticmethod(lambda connector_name: {"connector": connector_name}),
        raising=False,
    )
    monkeypatch.setattr(
        module.AllConnectorSettings,
        "update_connector_config_keys",
        staticmethod(lambda value: None),
        raising=False,
    )

    class Config:
        def __init__(self, value):
            self.connector = value["connector"]

    candidate = object()
    initialize_entered = asyncio.Event()
    discarded = []

    def login(**kwargs):
        security._secure_configs.clear()
        security._secure_configs["account-a"] = "LOGIN-PARTIAL"
        decryption_done.clear()
        return True

    async def initialize(*args):
        initialize_entered.set()
        await asyncio.Event().wait()

    async def stop_candidate(*args, **kwargs):
        discarded.append(candidate)

    monkeypatch.setattr(
        security, "login_account", staticmethod(login), raising=False
    )
    monkeypatch.setattr(module, "HummingbotAPIConfigAdapter", Config)
    monkeypatch.setattr(
        module, "api_keys_from_connector_config_map", lambda value: {}
    )
    service._create_trading_connector = lambda *args, **kwargs: candidate
    service._initialize_trading_connector_candidate = initialize
    service._stop_connector_network = stop_candidate

    async def stage():
        async with service.credential_transaction("account-a", "manzana"):
            await service.stage_connector_keys_already_locked(
                "account-a", "manzana", {"api_key": "NEW"}
            )

    operation = asyncio.create_task(stage())
    await initialize_entered.wait()
    operation.cancel("cancel-real-staging")
    with pytest.raises(asyncio.CancelledError) as captured:
        await asyncio.wait_for(operation, 0.2)

    CHECK.assertEqual(("cancel-real-staging",), captured.value.args)
    CHECK.assertEqual([candidate], discarded)
    CHECK.assertEqual(baseline, security._secure_configs)
    CHECK.assertTrue(decryption_done.is_set())


@pytest.mark.asyncio
async def test_issue224_global_security_lock_reentry_and_waiter_cancellation():
    module, registry_type = _load_unified_service_module()
    service = _final_boundary_service(module, registry_type)
    waiter_entered = asyncio.Event()

    async def waiter():
        async with service._backend_api_security_transaction():
            waiter_entered.set()

    async with service._backend_api_security_transaction():
        owner = asyncio.current_task()
        CHECK.assertIs(owner, module._BACKEND_API_SECURITY_TRANSACTION_OWNER)
        CHECK.assertEqual(1, module._BACKEND_API_SECURITY_TRANSACTION_DEPTH)
        async with service._backend_api_security_transaction():
            CHECK.assertIs(
                owner, module._BACKEND_API_SECURITY_TRANSACTION_OWNER
            )
            CHECK.assertEqual(2, module._BACKEND_API_SECURITY_TRANSACTION_DEPTH)

        queued = asyncio.create_task(waiter())
        await asyncio.sleep(0)
        CHECK.assertFalse(waiter_entered.is_set())
        queued.cancel("cancel-global-waiter")
        with pytest.raises(asyncio.CancelledError) as captured:
            await queued
        CHECK.assertEqual(("cancel-global-waiter",), captured.value.args)
        CHECK.assertIs(owner, module._BACKEND_API_SECURITY_TRANSACTION_OWNER)

    successor = asyncio.create_task(waiter())
    await asyncio.wait_for(successor, 0.2)
    CHECK.assertTrue(waiter_entered.is_set())
    CHECK.assertIsNone(module._BACKEND_API_SECURITY_TRANSACTION_OWNER)
    CHECK.assertEqual(0, module._BACKEND_API_SECURITY_TRANSACTION_DEPTH)
    CHECK.assertFalse(module._BACKEND_API_SECURITY_TRANSACTION_LOCK.locked())


@pytest.mark.asyncio
async def test_issue224_accounts_stop_waits_for_live_candidate_cleanup(
    monkeypatch,
):
    from contextlib import asynccontextmanager

    accounts_module = _load_accounts_service_module()
    token_read_entered = asyncio.Event()
    discard_entered = asyncio.Event()
    release_discard = asyncio.Event()
    events = []
    candidate = object()

    class ConnectorService:
        @asynccontextmanager
        async def credential_transaction(self, account_name, connector_name):
            yield

        def is_trading_connector_initialized(self, *args):
            return False

        async def stage_connector_keys_already_locked(self, *args):
            return {"connector": candidate, "committed": False}

        async def commit_staged_connector_keys_already_locked(self, *args):
            events.append("publish")

        async def discard_staged_connector_already_locked(self, *args):
            events.append("discard-enter")
            discard_entered.set()
            await release_discard.wait()
            events.append("discard-exit")

        async def stop_all(self):
            CHECK.assertFalse(service._account_continuations)
            events.append("stop-all")

    class WalletService:
        async def close(self):
            events.append("wallet-close")

    service = object.__new__(accounts_module.AccountsService)
    service._connector_service = ConnectorService()
    service.gateway_wallet_service = WalletService()
    service.accounts_state = {}
    service._update_account_state_task = None
    service._order_status_polling_task = None
    service._gateway_poller_start_task = None
    service._gateway_poller_started = False

    async def blocked_token_read(*args, **kwargs):
        token_read_entered.set()
        await asyncio.Event().wait()

    service._get_connector_tokens_info = blocked_token_read
    monkeypatch.setattr(
        accounts_module.fs_util,
        "path_exists",
        lambda path: False,
        raising=False,
    )

    caller = asyncio.create_task(
        service.add_credentials("acct", "manzana", {"api_key": "NEW"})
    )
    await token_read_entered.wait()
    shutdown = asyncio.create_task(service.stop())
    await discard_entered.wait()
    CHECK.assertFalse(shutdown.done())

    with pytest.raises(RuntimeError, match="accounts_service_stopping"):
        service.add_account("late-account")

    release_discard.set()
    await asyncio.wait_for(shutdown, 0.2)
    with pytest.raises(asyncio.CancelledError):
        await caller

    CHECK.assertEqual(
        ["discard-enter", "discard-exit", "wallet-close", "stop-all"],
        events,
    )
    CHECK.assertFalse(service._account_continuations)
    CHECK.assertTrue(service._stopping)
    CHECK.assertTrue(service._stopped)
    with pytest.raises(RuntimeError, match="accounts_service_stopping"):
        await service.add_credentials(
            "acct", "manzana", {"api_key": "TOO-LATE"}
        )


def test_issue224_reserved_account_routes_declare_exact_typed_400_refs():
    from fastapi import FastAPI, HTTPException
    from fastapi.testclient import TestClient

    from models.connectors import CapabilityErrorResponse
    from routers import accounts as accounts_router
    from services.accounts_service import validate_safe_name

    class Accounts:
        def list_credentials(self, account_name):
            validate_safe_name(account_name, "account name")
            raise AssertionError("reserved account reached credential listing")

        def add_account(self, account_name):
            if account_name == "duplicate":
                raise HTTPException(status_code=400, detail="private failure")
            raise AssertionError("reserved account reached account creation")

    app = FastAPI()
    app.include_router(accounts_router.router)
    app.dependency_overrides[accounts_router.get_accounts_service] = Accounts
    client = TestClient(app)
    responses = (
        client.get("/accounts/__data__/credentials"),
        client.post(
            "/accounts/add-account", params={"account_name": "__data__"}
        ),
    )
    for response in responses:
        CHECK.assertEqual(400, response.status_code, response.text)
        CHECK.assertEqual(
            "invalid_account_resource_name", response.json()["detail"]["code"]
        )
        CapabilityErrorResponse.model_validate(response.json())

    duplicate = client.post(
        "/accounts/add-account", params={"account_name": "duplicate"}
    )
    CHECK.assertEqual(400, duplicate.status_code, duplicate.text)
    CHECK.assertEqual("account_create_failed", duplicate.json()["detail"]["code"])
    CHECK.assertNotIn("private failure", duplicate.text)
    CapabilityErrorResponse.model_validate(duplicate.json())

    paths = app.openapi()["paths"]
    route_methods = (
        ("/accounts/{account_name}/credentials", "get"),
        ("/accounts/add-account", "post"),
    )
    for path, method in route_methods:
        schema = paths[path][method]["responses"]["400"]["content"][
            "application/json"
        ]["schema"]
        CHECK.assertEqual(
            "#/components/schemas/CapabilityErrorResponse", schema["$ref"]
        )


def _load_gateway_transaction_poller_module():
    database = _module("database")
    database.AsyncDatabaseManager = type("AsyncDatabaseManager", (), {})
    gateway_client = _module("services.gateway_client")
    gateway_client.GatewayClient = type("GatewayClient", (), {})
    gateway_client.get_native_gas_token = lambda chain: "SOL"
    clmm_service = _module("services.gateway_clmm_service")
    clmm_service.GatewayCLMMService = type("GatewayCLMMService", (), {})
    swap_service = _module("services.gateway_swap_service")
    swap_service.GatewaySwapService = type("GatewaySwapService", (), {})

    module_name = "issue224_gateway_transaction_poller_under_test"
    spec = importlib.util.spec_from_file_location(
        module_name,
        Path(__file__).resolve().parents[1]
        / "services"
        / "gateway_transaction_poller.py",
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _issue224_publisher_probe_service(accounts_module, gateway_poller):
    events = []

    class ConnectorService:
        async def stop_all(self):
            events.append("connectors-stopped")

    class WalletService:
        async def close(self):
            events.append("wallet-closed")

    service = object.__new__(accounts_module.AccountsService)
    service._connector_service = ConnectorService()
    service.gateway_wallet_service = WalletService()
    service.gateway_tx_poller = gateway_poller
    service.accounts_state = {}
    service._update_account_state_task = None
    service._order_status_polling_task = None
    service._gateway_poller_start_task = None
    service._gateway_poller_started = False
    service._account_continuations = set()
    service._stopping = False
    service._stopped = False
    service._stop_in_progress = False
    service._stop_lock = asyncio.Lock()
    return service, events


@pytest.mark.asyncio
async def test_issue224_accounts_publishers_are_generation_safe_and_drained():
    accounts_module = _load_accounts_service_module()
    complete_first = asyncio.Event()
    fail_second = asyncio.Event()
    publisher_calls = 0
    owned_tasks = set()

    class GatewayPoller:
        async def start(self):
            return None

        async def stop(self):
            return None

    service, _ = _issue224_publisher_probe_service(
        accounts_module, GatewayPoller()
    )

    async def update_loop():
        nonlocal publisher_calls
        publisher_calls += 1
        generation = publisher_calls
        if generation == 1:
            await complete_first.wait()
            return "first-complete"
        if generation == 2:
            await fail_second.wait()
            raise RuntimeError("publisher-generation-failed")
        await asyncio.Event().wait()

    async def order_loop():
        await asyncio.Event().wait()

    service.update_account_state_loop = update_loop
    service.order_status_polling_loop = order_loop
    try:
        service.start()
        first_update = service._update_account_state_task
        first_order = service._order_status_polling_task
        owned_tasks.update((first_update, first_order))
        service.start()
        CHECK.assertIs(first_update, service._update_account_state_task)
        CHECK.assertIs(first_order, service._order_status_polling_task)

        complete_first.set()
        await first_update
        service.start()
        second_update = service._update_account_state_task
        owned_tasks.add(second_update)
        CHECK.assertIsNot(first_update, second_update)
        CHECK.assertIs(first_order, service._order_status_polling_task)

        fail_second.set()
        await asyncio.sleep(0)
        CHECK.assertTrue(second_update.done())
        service.start()
        third_update = service._update_account_state_task
        owned_tasks.add(third_update)
        CHECK.assertIsNot(second_update, third_update)
        CHECK.assertFalse(second_update._log_traceback)

        await service.stop()
        CHECK.assertTrue(all(task.done() for task in owned_tasks))
        CHECK.assertTrue(service._stopped)
    finally:
        for task in owned_tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*owned_tasks, return_exceptions=True)


@pytest.mark.asyncio
async def test_issue224_gateway_poller_stop_drains_both_tasks_before_failure():
    module = _load_gateway_transaction_poller_module()
    position_cancelled = asyncio.Event()

    async def failed_transaction_publisher():
        raise RuntimeError("transaction-publisher-failed")

    async def position_publisher():
        try:
            await asyncio.Event().wait()
        finally:
            position_cancelled.set()

    transaction_task = asyncio.create_task(failed_transaction_publisher())
    position_task = asyncio.create_task(position_publisher())
    await asyncio.sleep(0)
    poller = object.__new__(module.GatewayTransactionPoller)
    poller._running = True
    poller._poll_task = transaction_task
    poller._position_poll_task = position_task
    try:
        with pytest.raises(RuntimeError, match="transaction-publisher-failed"):
            await poller.stop()
        CHECK.assertTrue(position_cancelled.is_set())
        CHECK.assertTrue(transaction_task.done())
        CHECK.assertTrue(position_task.done())
        CHECK.assertFalse(transaction_task._log_traceback)
        CHECK.assertIsNone(poller._poll_task)
        CHECK.assertIsNone(poller._position_poll_task)
    finally:
        for task in (transaction_task, position_task):
            if not task.done():
                task.cancel()
        await asyncio.gather(
            transaction_task, position_task, return_exceptions=True
        )


@pytest.mark.asyncio
async def test_issue224_gateway_start_failure_retries_without_replacing_publishers():
    accounts_module = _load_accounts_service_module()
    start_calls = 0
    owned_tasks = set()

    class GatewayPoller:
        async def start(self):
            nonlocal start_calls
            start_calls += 1
            if start_calls == 1:
                raise RuntimeError("gateway-start-failed")

        async def stop(self):
            return None

    service, _ = _issue224_publisher_probe_service(
        accounts_module, GatewayPoller()
    )

    async def publisher():
        await asyncio.Event().wait()

    service.update_account_state_loop = publisher
    service.order_status_polling_loop = publisher
    try:
        service.start()
        update_task = service._update_account_state_task
        order_task = service._order_status_polling_task
        owned_tasks.update((update_task, order_task))
        await asyncio.sleep(0)
        CHECK.assertEqual(1, start_calls)
        CHECK.assertFalse(service._gateway_poller_started)
        CHECK.assertIsNone(service._gateway_poller_start_task)

        service.start()
        CHECK.assertIs(update_task, service._update_account_state_task)
        CHECK.assertIs(order_task, service._order_status_polling_task)
        await asyncio.sleep(0)
        CHECK.assertEqual(2, start_calls)
        CHECK.assertTrue(service._gateway_poller_started)
        CHECK.assertIsNone(service._gateway_poller_start_task)
        await service.stop()
    finally:
        for task in owned_tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*owned_tasks, return_exceptions=True)


@pytest.mark.asyncio
async def test_issue224_accounts_stop_propagates_poller_failure_after_terminal_work():
    accounts_module = _load_accounts_service_module()
    publisher_tasks = []

    class GatewayPoller:
        async def start(self):
            return None

        async def stop(self):
            CHECK.assertTrue(all(task.done() for task in publisher_tasks))
            raise RuntimeError("gateway-poller-stop-failed")

    service, events = _issue224_publisher_probe_service(
        accounts_module, GatewayPoller()
    )

    async def publisher():
        await asyncio.Event().wait()

    service.update_account_state_loop = publisher
    service.order_status_polling_loop = publisher
    service.start()
    publisher_tasks.extend(
        (
            service._update_account_state_task,
            service._order_status_polling_task,
        )
    )
    await asyncio.sleep(0)
    try:
        with pytest.raises(RuntimeError, match="gateway-poller-stop-failed"):
            await service.stop()
        CHECK.assertTrue(all(task.done() for task in publisher_tasks))
        CHECK.assertEqual(
            ["wallet-closed", "connectors-stopped"], events
        )
        CHECK.assertFalse(service._stopped)
    finally:
        for task in publisher_tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*publisher_tasks, return_exceptions=True)


@pytest.mark.asyncio
async def test_issue224_executor_account_and_config_errors_are_typed_and_pure():
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from models.connectors import CapabilityErrorResponse
    from models.executors import CreateExecutorRequest
    from routers import executors as executors_router
    from services.connector_capabilities import ConnectorCapabilityError

    route_calls = []

    class RouteService:
        async def create_executor(self, **kwargs):
            route_calls.append(kwargs)
            raise AssertionError("reserved account reached executor service")

    app = FastAPI()
    app.include_router(executors_router.router)
    app.dependency_overrides[executors_router.get_executor_service] = RouteService
    client = TestClient(app)
    reserved = client.post(
        "/executors/",
        json={"account_name": "__data__", "executor_config": {}},
    )
    CHECK.assertEqual(400, reserved.status_code, reserved.text)
    CHECK.assertEqual([], route_calls)
    CHECK.assertEqual(
        "invalid_account_resource_name", reserved.json()["detail"]["code"]
    )
    CapabilityErrorResponse.model_validate(reserved.json())
    with pytest.raises(ConnectorCapabilityError):
        CreateExecutorRequest(
            account_name="__data__", executor_config={}
        )

    module = _load_executor_service_module()
    interface_calls = []

    class ConnectorService:
        def require_capability(self, *args, **kwargs):
            return None

    class TradingService:
        connector_service = ConnectorService()

        def get_trading_interface(self, account_name):
            interface_calls.append(account_name)
            return types.SimpleNamespace(current_timestamp=123.0)

    service = object.__new__(module.ExecutorService)
    service._trading_service = TradingService()
    service._trading_interfaces = {}
    service.default_account = "master_account"

    invalid_cases = (
        ({}, "missing_type"),
        ({"type": "PRIVATE-INVALID-TYPE"}, "unsupported_type"),
    )
    for config, reason in invalid_cases:
        with pytest.raises(ConnectorCapabilityError) as captured:
            await service.create_executor(config, account_name="master_account")
        CHECK.assertEqual(reason, captured.value.detail["reason"])
        CHECK.assertNotIn("PRIVATE", json.dumps(captured.value.detail))
        CapabilityErrorResponse.model_validate(
            {"detail": captured.value.detail}
        )

    class StrictConfig(BaseModel):
        amount: int

    service.EXECUTOR_REGISTRY = {
        "position_executor": (object, StrictConfig)
    }
    with pytest.raises(ConnectorCapabilityError) as invalid_config:
        await service.create_executor(
            {"type": "position_executor", "amount": "PRIVATE-CONFIG"},
            account_name="master_account",
        )
    CHECK.assertEqual(
        "validation_failed", invalid_config.value.detail["reason"]
    )
    CHECK.assertNotIn("PRIVATE-CONFIG", json.dumps(invalid_config.value.detail))
    CHECK.assertEqual([], interface_calls)
    CHECK.assertEqual({}, service._trading_interfaces)

    responses = app.openapi()["paths"]["/executors/"]["post"]["responses"]
    CHECK.assertEqual(
        "#/components/schemas/CapabilityErrorResponse",
        responses["400"]["content"]["application/json"]["schema"]["$ref"],
    )
