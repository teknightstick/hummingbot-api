"""
Pydantic models for the connectors router.

These models define the request/response schemas for connector-related endpoints.
"""

from typing import Any, Dict, FrozenSet, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_serializer


class ConnectorInfo(BaseModel):
    """Information about a connector"""
    name: str = Field(description="Connector name")
    is_perpetual: bool = Field(default=False, description="Whether the connector supports perpetual trading")
    supported_order_types: Optional[List[str]] = Field(default=None, description="Supported order types")


class ConnectorCapabilities(BaseModel):
    """Connector-declared API semantics.

    ``declared=False`` is intentionally different from claiming every capability. It means
    the connector predates this seam, so the API preserves its existing generic behavior.
    """

    model_config = ConfigDict(frozen=True)

    declared: bool = Field(default=False, description="Whether the capability set is explicit")
    source: str = Field(default="undeclared", description="Origin of the declaration")
    supported_pairs: Optional[FrozenSet[str]] = Field(
        default=None, description="Static supported pairs, or null when the connector is not pair-bounded"
    )
    features: FrozenSet[str] = Field(
        default_factory=frozenset,
        description=(
            "Explicitly supported API features; order_book is snapshot access, while "
            "live_order_book is public stream readiness"
        ),
    )
    dynamic_subscription: Optional[bool] = Field(
        default=None, description="Whether pairs may be subscribed or removed dynamically"
    )
    perpetual: Optional[bool] = Field(default=None, description="Whether perpetual reads are supported")
    candles: Optional[bool] = Field(default=None, description="Whether candle reads are supported")
    last_trade: Optional[bool] = Field(
        default=None, description="Whether the connector exposes an actual last-trade price"
    )

    @field_serializer("supported_pairs", "features", when_used="json")
    def serialize_ordered_membership(self, value):
        """Keep set-like validation while making every public JSON array deterministic."""
        return None if value is None else sorted(value)


class CapabilityErrorDetail(BaseModel):
    """Stable detail object returned for an unsupported connector operation."""

    code: str
    message: str
    connector: str
    feature: str
    trading_pair: Optional[str] = None
    supported_pairs: Optional[List[str]] = None
    reason: Optional[str] = None
    cleanup_scope: Optional[Literal["pair", "connector"]] = Field(
        default=None,
        description="Stable ownership scope for an incomplete cleanup",
    )
    cleanup_code: Optional[str] = Field(
        default=None,
        description="Sanitized stable cleanup outcome code",
    )


class CapabilityErrorResponse(BaseModel):
    """FastAPI error envelope for capability rejections."""

    detail: CapabilityErrorDetail


class PublicErrorDetail(BaseModel):
    """Stable sanitized detail for non-capability public failures."""

    code: str
    message: str


class PublicErrorResponse(BaseModel):
    """FastAPI envelope for stable sanitized public failures."""

    detail: PublicErrorDetail


class TradingRuleError(BaseModel):
    """One requested pair whose rule was unavailable."""

    code: str = "trading_pair_not_found"
    message: str
    trading_pair: str


class TradingRulesPartialResponse(BaseModel):
    """HTTP 207 result with successful rules separated from typed pair errors."""

    connector: str
    rules: Dict[str, Dict[str, Any]]
    errors: Dict[str, TradingRuleError]


class TradingRulesNotFoundDetail(BaseModel):
    code: str = "trading_rules_not_found"
    message: str
    connector: str
    trading_pairs: List[str]


class TradingRulesNotFoundResponse(BaseModel):
    detail: TradingRulesNotFoundDetail


class ConnectorConfigMapResponse(BaseModel):
    """Response for connector configuration requirements"""
    connector_name: str = Field(description="Name of the connector")
    config_fields: List[str] = Field(description="List of required configuration fields")


class TradingRule(BaseModel):
    """Trading rules for a specific trading pair"""
    min_order_size: float = Field(description="Minimum order size")
    max_order_size: float = Field(description="Maximum order size")
    min_price_increment: float = Field(description="Minimum price increment")
    min_base_amount_increment: float = Field(description="Minimum base amount increment")
    min_quote_amount_increment: float = Field(description="Minimum quote amount increment")
    min_notional_size: float = Field(description="Minimum notional size")
    min_order_value: float = Field(description="Minimum order value")
    max_price_significant_digits: float = Field(description="Maximum price significant digits")
    supports_limit_orders: bool = Field(description="Whether limit orders are supported")
    supports_market_orders: bool = Field(description="Whether market orders are supported")
    buy_order_collateral_token: str = Field(description="Collateral token for buy orders")
    sell_order_collateral_token: str = Field(description="Collateral token for sell orders")


class ConnectorTradingRulesResponse(BaseModel):
    """Response for connector trading rules"""
    connector: str = Field(description="Connector name")
    trading_pairs: Optional[List[str]] = Field(default=None, description="Filtered trading pairs if provided")
    rules: Dict[str, TradingRule] = Field(description="Trading rules by trading pair")


class ConnectorOrderTypesResponse(BaseModel):
    """Response for supported order types"""
    connector: str = Field(description="Connector name")
    supported_order_types: List[str] = Field(description="List of supported order types")


class ConnectorListResponse(BaseModel):
    """Response for list of available connectors"""
    connectors: List[str] = Field(description="List of available connector names")
    count: int = Field(description="Total number of connectors")
