"""API-local command semantics for Issue #222.

The production image supplies Hummingbot's compiled package.  The isolated Windows
contract runner used for this issue does not, so the small import shim below exposes
only the names needed to import the API service.  Behavioral fakes implement the
same pinned OrderBook primitive contracts exercised by each test.
"""

import importlib
import sys
import types
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from pathlib import Path

import pytest
from pydantic import BaseModel


def _module(name: str) -> types.ModuleType:
    module = sys.modules.get(name)
    if module is None:
        module = types.ModuleType(name)
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


_install_import_contract()

from services.market_data_service import MarketDataService
from models.market_data import OrderBookQueryResult


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
        assert is_buy is True
        cumulative = 0.0
        result_price = float("nan")
        for price, amount in self.asks:
            cumulative += price * amount
            if cumulative >= quote_volume:
                result_price = price
                break
        return QueryResult(float("nan"), quote_volume, result_price, min(cumulative, quote_volume))

    def get_quote_volume_for_price(self, is_buy: bool, price: float) -> QueryResult:
        assert is_buy is True
        cumulative = 0.0
        result_price = float("nan")
        for level_price, amount in self.asks:
            if level_price > price:
                break
            cumulative += level_price * amount
            result_price = level_price
        return QueryResult(price, float("nan"), result_price, cumulative)

    def get_vwap_for_volume(self, is_buy: bool, volume: float) -> QueryResult:
        assert is_buy is True
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
        assert connector_name == "static"
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

    assert price_for_quote == {
        "trading_pair": "BTC-USD",
        "is_buy": True,
        "query_volume": 150.0,
        "result_price": 110.0,
        "result_quote_volume": 150.0,
        "timestamp": price_for_quote["timestamp"],
    }
    assert quote_for_price == {
        "trading_pair": "BTC-USD",
        "is_buy": True,
        "query_price": 110.0,
        "result_price": 110.0,
        "result_quote_volume": 210.0,
        "timestamp": quote_for_price["timestamp"],
    }


@pytest.mark.asyncio
async def test_vwap_price_is_canonical_in_service_and_openapi_model():
    service = MarketDataService(OrderBookConnectorService(TwoLevelOrderBook()))

    result = await service.get_order_book_query_result(
        "static", "BTC-USD", True, vwap_volume=2.0
    )

    assert result["result_price"] == 105.0
    assert "average_price" not in result
    schema_properties = OrderBookQueryResult.model_json_schema()["properties"]
    assert "result_price" in schema_properties
    assert "average_price" not in schema_properties
    assert "result_price" in OrderBookQueryResult.model_json_schema()["required"]
    assert schema_properties["result_price"]["type"] == "number"
    assert OrderBookQueryResult(**result).result_price == 105.0


def test_connector_capability_registry_is_declared_and_noninvasive():
    from models.connectors import ConnectorCapabilities
    from services.connector_capabilities import ConnectorCapabilityRegistry, ConnectorFeature

    registry = ConnectorCapabilityRegistry()
    manzana = registry.get("manzana")

    assert manzana.declared is True
    assert manzana.supported_pairs == frozenset({"BTC-USD"})
    assert manzana.dynamic_subscription is False
    assert manzana.perpetual is False
    assert manzana.candles is False
    assert manzana.last_trade is False
    assert ConnectorFeature.ORDER_BOOK in manzana.features
    assert registry.supports_pair("manzana", "BTC-USD") is True
    assert registry.supports_pair("manzana", "ETH-USD") is False

    undeclared = registry.get("binance")
    assert undeclared.declared is False
    assert registry.supports_pair("binance", "ANY-PAIR") is True
    assert registry.supports_feature("binance", ConnectorFeature.CANDLES) is True

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
    assert declared.declared is True
    assert declared.supported_pairs == frozenset({"ONE-USD"})
    assert registry.supports_pair("custom", "ONE-USD", ConnectorDeclared()) is True


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

    assert capabilities.declared is True
    assert capabilities.source == "api-local-pending-connector-seam"
    assert capabilities.supported_pairs == frozenset({"BTC-USD"})


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
    assert pair_error.value.status_code == 400
    assert pair_error.value.detail == {
        "code": "unsupported_trading_pair",
        "message": "Connector 'manzana' does not support trading pair 'ETH-USD'",
        "connector": "manzana",
        "feature": ConnectorFeature.ORDER_BOOK.value,
        "trading_pair": "ETH-USD",
        "supported_pairs": ["BTC-USD"],
    }
    CapabilityErrorResponse.model_validate({"detail": pair_error.value.detail})

    with pytest.raises(ConnectorCapabilityError) as feature_error:
        await service.get_prices("manzana", ["BTC-USD"])
    assert feature_error.value.status_code == 400
    assert feature_error.value.detail["code"] == "unsupported_connector_feature"
    assert feature_error.value.detail["feature"] == ConnectorFeature.LAST_TRADE.value

    with pytest.raises(ConnectorCapabilityError) as readiness_error:
        await service.initialize_order_book("manzana", "BTC-USD")
    assert readiness_error.value.status_code == 400
    assert readiness_error.value.detail["feature"] == "live_order_book"
    assert connector_service.connector_lookups == 0


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
    assert partial.status_code == 207
    partial_body = json.loads(partial.body)
    assert partial_body == {
        "connector": "generic",
        "rules": {"BTC-USD": valid_rule},
        "errors": {
            "ETH-USD": {
                "code": "trading_pair_not_found",
                "message": "Trading pair 'ETH-USD' is not available",
                "trading_pair": "ETH-USD",
            }
        },
    }
    assert "KeyError" not in partial.body.decode()

    with pytest.raises(HTTPException) as missing:
        await get_trading_rules(request, "generic", ["ETH-USD"])
    assert missing.value.status_code == 404
    assert missing.value.detail == {
        "code": "trading_rules_not_found",
        "message": "No trading rules found for the requested pairs",
        "connector": "generic",
        "trading_pairs": ["ETH-USD"],
    }


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
    assert single.value.status_code == 404
    assert single.value.detail == {
        "code": "connector_rate_not_found",
        "message": "Rate not found for connector 'manzana'",
        "connector": "manzana",
        "trading_pairs": ["BTC-USD"],
    }
    RateNotFoundResponse.model_validate({"detail": single.value.detail})

    with pytest.raises(HTTPException) as batch:
        await get_rates(
            RateRequest(trading_pairs=["BTC-USD", "ETH-USD"], connector="manzana"),
            service,
        )
    assert batch.value.status_code == 404
    assert batch.value.detail["trading_pairs"] == ["BTC-USD", "ETH-USD"]

    unscoped = await get_single_rate("BTC-USD", None, service)
    assert unscoped.rate is None


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
    assert positions_error.value.status_code == 400
    assert positions_error.value.detail["code"] == "unsupported_connector_feature"
    assert positions_error.value.detail["feature"] == "perpetual"

    with pytest.raises(HTTPException) as funding_error:
        await get_funding_payments(
            FundingPaymentFilterRequest(
                account_names=["master"], connector_names=["manzana"]
            ),
            history_service,
            connector_service,
        )
    assert funding_error.value.status_code == 400
    assert history_service.get_funding_payments.await_count == 0
    assert accounts_service.get_account_positions.await_count == 0

    global_positions = await get_positions(
        PositionFilterRequest(), accounts_service, connector_service
    )
    global_funding = await get_funding_payments(
        FundingPaymentFilterRequest(), history_service, connector_service
    )
    assert global_positions.data == []
    assert global_funding.data == []


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

    assert interface.markets == expected_markets


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

    assert unsupported.value.status_code == 400
    assert unsupported.value.detail["feature"] == "dynamic_subscription"
    assert connector_service.initialize_calls == 0


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

    assert unsupported.value.status_code == 400
    assert unsupported.value.detail["feature"] == "candles"


def test_openapi_documents_capability_partial_and_canonical_price_contracts():
    from fastapi import FastAPI

    from routers import connectors, market_data, trading

    app = FastAPI()
    app.include_router(connectors.router)
    app.include_router(market_data.router)
    app.include_router(trading.router)
    schema = app.openapi()

    query_schema = schema["components"]["schemas"]["OrderBookQueryResult"]
    assert "result_price" in query_schema["required"]
    assert "average_price" not in query_schema["properties"]
    assert (
        schema["paths"]["/connectors/{connector_name}/capabilities"]["get"]
        ["responses"]["200"]["content"]["application/json"]["schema"]["$ref"]
        == "#/components/schemas/ConnectorCapabilities"
    )
    trading_rule_responses = schema["paths"][
        "/connectors/{connector_name}/trading-rules"
    ]["get"]["responses"]
    assert {"207", "400", "404"}.issubset(trading_rule_responses)
