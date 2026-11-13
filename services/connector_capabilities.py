"""Reusable connector capability declarations for API-local validation.

Connector implementations can expose an ``api_capabilities`` class or instance attribute
using the same schema. The API-local declaration below is the temporary seam for the pinned
Manzana connector until child B/C publish an accepted connector interface.
"""

from enum import Enum
from typing import Any, Dict, Mapping, Optional

from fastapi import HTTPException

from models.connectors import CapabilityErrorDetail, ConnectorCapabilities


class ConnectorFeature(str, Enum):
    ORDER_BOOK = "order_book"
    ORDER_BOOK_QUERY = "order_book_query"
    LIVE_ORDER_BOOK = "live_order_book"
    TRADING_RULES = "trading_rules"
    TICKERS = "tickers"
    BEST_BID_ASK = "best_bid_ask"
    RATES = "rates"
    DYNAMIC_SUBSCRIPTION = "dynamic_subscription"
    PERPETUAL = "perpetual"
    CANDLES = "candles"
    LAST_TRADE = "last_trade"
    ORDER_SUBMISSION = "order_submission"


class ConnectorCapabilityError(HTTPException):
    """Typed HTTP 400 raised before an unsupported request reaches connector state."""

    def __init__(self, detail: CapabilityErrorDetail):
        super().__init__(
            status_code=400,
            detail=detail.model_dump(exclude_none=True),
        )


_API_LOCAL_DECLARATIONS: Dict[str, ConnectorCapabilities] = {
    "manzana": ConnectorCapabilities(
        declared=True,
        source="api-local-pending-connector-seam",
        supported_pairs=frozenset({"BTC-USD"}),
        features=frozenset(
            {
                ConnectorFeature.ORDER_BOOK.value,
                ConnectorFeature.ORDER_BOOK_QUERY.value,
                ConnectorFeature.TRADING_RULES.value,
                ConnectorFeature.RATES.value,
            }
        ),
        dynamic_subscription=False,
        perpetual=False,
        candles=False,
        last_trade=False,
    )
}


class ConnectorCapabilityRegistry:
    """Resolve API-local or connector-declared capability contracts."""

    def __init__(self, declarations: Optional[Mapping[str, ConnectorCapabilities]] = None):
        self._declarations = dict(_API_LOCAL_DECLARATIONS)
        if declarations:
            self._declarations.update(declarations)

    def register(self, connector_name: str, capabilities: ConnectorCapabilities) -> None:
        self._declarations[connector_name] = capabilities.model_copy(
            update={"declared": True, "source": capabilities.source or "registered"}
        )

    def get(self, connector_name: str, connector: Any = None) -> ConnectorCapabilities:
        declaration = getattr(connector, "api_capabilities", None) if connector is not None else None
        if declaration is not None:
            if isinstance(declaration, ConnectorCapabilities):
                return declaration.model_copy(update={"declared": True, "source": "connector"})
            return ConnectorCapabilities.model_validate(
                {**dict(declaration), "declared": True, "source": "connector"}
            )

        declared = self._declarations.get(connector_name)
        if declared is not None:
            return declared
        return ConnectorCapabilities()

    def supports_pair(self, connector_name: str, trading_pair: str, connector: Any = None) -> bool:
        capabilities = self.get(connector_name, connector)
        return (
            not capabilities.declared
            or capabilities.supported_pairs is None
            or trading_pair in capabilities.supported_pairs
        )

    def supports_feature(
        self, connector_name: str, feature: ConnectorFeature, connector: Any = None
    ) -> bool:
        capabilities = self.get(connector_name, connector)
        if not capabilities.declared:
            return True

        flag = {
            ConnectorFeature.DYNAMIC_SUBSCRIPTION: capabilities.dynamic_subscription,
            ConnectorFeature.PERPETUAL: capabilities.perpetual,
            ConnectorFeature.CANDLES: capabilities.candles,
            ConnectorFeature.LAST_TRADE: capabilities.last_trade,
        }.get(feature)
        if flag is not None:
            return flag
        return feature.value in capabilities.features

    def require(
        self,
        connector_name: str,
        feature: ConnectorFeature,
        trading_pair: Optional[str] = None,
        connector: Any = None,
    ) -> None:
        capabilities = self.get(connector_name, connector)
        if trading_pair is not None and not self.supports_pair(
            connector_name, trading_pair, connector
        ):
            raise ConnectorCapabilityError(
                CapabilityErrorDetail(
                    code="unsupported_trading_pair",
                    message=(
                        f"Connector '{connector_name}' does not support trading pair "
                        f"'{trading_pair}'"
                    ),
                    connector=connector_name,
                    feature=feature.value,
                    trading_pair=trading_pair,
                    supported_pairs=sorted(capabilities.supported_pairs or ()),
                )
            )
        if not self.supports_feature(connector_name, feature, connector):
            raise ConnectorCapabilityError(
                CapabilityErrorDetail(
                    code="unsupported_connector_feature",
                    message=f"Connector '{connector_name}' does not support feature '{feature.value}'",
                    connector=connector_name,
                    feature=feature.value,
                    trading_pair=trading_pair,
                    supported_pairs=(
                        sorted(capabilities.supported_pairs)
                        if capabilities.supported_pairs is not None
                        else None
                    ),
                )
            )
