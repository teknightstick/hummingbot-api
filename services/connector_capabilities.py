"""Immutable bootstrap and instance connector capability declarations.

Bootstrap declarations let gates answer before connector construction. Connector
implementations can then expose an ``api_capabilities`` class or instance attribute using
the same schema. A loaded instance may narrow discovered pairs, but it may not silently
change the bootstrap feature contract.
"""

from enum import Enum
from types import MappingProxyType
from typing import Any, Mapping, Optional

from pydantic import ValidationError

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


_BOOTSTRAP_DECLARATIONS: Mapping[str, ConnectorCapabilities] = MappingProxyType(
    {
        "manzana": ConnectorCapabilities(
            declared=True,
            source="api-bootstrap",
            supported_pairs=None,
            features=frozenset(
                {
                    ConnectorFeature.ORDER_BOOK.value,
                    ConnectorFeature.ORDER_BOOK_QUERY.value,
                    ConnectorFeature.LIVE_ORDER_BOOK.value,
                    ConnectorFeature.TRADING_RULES.value,
                    ConnectorFeature.BEST_BID_ASK.value,
                    ConnectorFeature.DYNAMIC_SUBSCRIPTION.value,
                }
            ),
            dynamic_subscription=True,
            perpetual=False,
            candles=False,
            last_trade=False,
        )
    }
)


class ConnectorCapabilityRegistry:
    """Resolve immutable bootstrap declarations and compatible instance truth."""

    def __init__(self, declarations: Optional[Mapping[str, ConnectorCapabilities]] = None):
        bootstrap = dict(_BOOTSTRAP_DECLARATIONS)
        if declarations:
            bootstrap.update(declarations)
        self._bootstrap_declarations = MappingProxyType(
            {
                connector_name: self._normalize_declaration(
                    capabilities, source="bootstrap"
                )
                for connector_name, capabilities in bootstrap.items()
            }
        )

    @staticmethod
    def _normalize_declaration(declaration: Any, *, source: str) -> ConnectorCapabilities:
        if isinstance(declaration, ConnectorCapabilities):
            normalized_source = declaration.source
            if normalized_source == "undeclared":
                normalized_source = source
            return declaration.model_copy(
                update={"declared": True, "source": normalized_source}
            )
        return ConnectorCapabilities.model_validate(
            {**dict(declaration), "declared": True, "source": source}
        )

    @staticmethod
    def _semantic_features(capabilities: ConnectorCapabilities) -> frozenset[str]:
        """Canonicalize redundant flag/feature representations for comparison."""
        features = set(capabilities.features)
        for feature, flag in (
            (ConnectorFeature.DYNAMIC_SUBSCRIPTION, capabilities.dynamic_subscription),
            (ConnectorFeature.PERPETUAL, capabilities.perpetual),
            (ConnectorFeature.CANDLES, capabilities.candles),
            (ConnectorFeature.LAST_TRADE, capabilities.last_trade),
        ):
            if flag is True:
                features.add(feature.value)
            elif flag is False:
                features.discard(feature.value)
        return frozenset(features)

    @classmethod
    def _semantically_compatible(
        cls,
        bootstrap: ConnectorCapabilities,
        instance: ConnectorCapabilities,
    ) -> bool:
        if cls._semantic_features(bootstrap) != cls._semantic_features(instance):
            return False
        if bootstrap.supported_pairs is None:
            return True
        return (
            instance.supported_pairs is not None
            and instance.supported_pairs.issubset(bootstrap.supported_pairs)
        )

    @staticmethod
    def _raise_mismatch(connector_name: str) -> None:
        raise ConnectorCapabilityError(
            CapabilityErrorDetail(
                code="connector_capability_mismatch",
                message=(
                    f"Connector '{connector_name}' capability declaration does not "
                    "match its bootstrap declaration"
                ),
                connector=connector_name,
                feature="capabilities",
                reason="bootstrap_instance_incompatible",
            )
        )

    def _instance_declaration(
        self, connector_name: str, connector: Any
    ) -> Optional[ConnectorCapabilities]:
        declaration = getattr(connector, "api_capabilities", None)
        if declaration is None:
            return None
        try:
            return self._normalize_declaration(declaration, source="connector")
        except (TypeError, ValueError, ValidationError):
            self._raise_mismatch(connector_name)

    def get(self, connector_name: str, connector: Any = None) -> ConnectorCapabilities:
        bootstrap = self._bootstrap_declarations.get(connector_name)
        if connector is None:
            return bootstrap if bootstrap is not None else ConnectorCapabilities()

        instance = self._instance_declaration(connector_name, connector)
        if bootstrap is not None:
            if instance is None or not self._semantically_compatible(
                bootstrap, instance
            ):
                self._raise_mismatch(connector_name)
            return instance
        if instance is not None:
            return instance
        return ConnectorCapabilities()

    def validate_instance(self, connector_name: str, connector: Any) -> None:
        """Fail closed before a constructed connector is published in a cache."""
        self.get(connector_name, connector)

    @staticmethod
    def _supports_pair(
        capabilities: ConnectorCapabilities, trading_pair: str
    ) -> bool:
        return (
            not capabilities.declared
            or capabilities.supported_pairs is None
            or trading_pair in capabilities.supported_pairs
        )

    def supports_pair(self, connector_name: str, trading_pair: str, connector: Any = None) -> bool:
        capabilities = self.get(connector_name, connector)
        return self._supports_pair(capabilities, trading_pair)

    @classmethod
    def _supports_feature(
        cls, capabilities: ConnectorCapabilities, feature: ConnectorFeature
    ) -> bool:
        if not capabilities.declared:
            return True
        return feature.value in cls._semantic_features(capabilities)

    def supports_feature(
        self, connector_name: str, feature: ConnectorFeature, connector: Any = None
    ) -> bool:
        capabilities = self.get(connector_name, connector)
        return self._supports_feature(capabilities, feature)

    def require(
        self,
        connector_name: str,
        feature: ConnectorFeature,
        trading_pair: Optional[str] = None,
        connector: Any = None,
    ) -> None:
        capabilities = self.get(connector_name, connector)
        if trading_pair is not None and not self._supports_pair(
            capabilities, trading_pair
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
        if not self._supports_feature(capabilities, feature):
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
