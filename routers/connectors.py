from typing import Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from hummingbot.client.settings import AllConnectorSettings

from deps import get_accounts_service
from models.connectors import (
    CapabilityErrorResponse,
    ConnectorCapabilities,
    TradingRuleError,
    TradingRulesNotFoundDetail,
    TradingRulesNotFoundResponse,
    TradingRulesPartialResponse,
)
from services.accounts_service import AccountsService
from services.market_data_service import MarketDataService

router = APIRouter(tags=["Connectors"], prefix="/connectors")


@router.get("/", response_model=List[str])
async def available_connectors():
    """
    Get a list of all available connectors.

    Returns:
        List of connector names supported by the system (excludes DEX providers which use Gateway networks)
    """
    all_connectors = AllConnectorSettings.get_connector_settings().keys()
    # Filter out DEX providers (contain '/') - these are accessed via Gateway networks
    return [c for c in all_connectors if '/' not in c]


@router.get("/{connector_name}/config-map", response_model=Dict[str, dict])
async def get_connector_config_map(connector_name: str, accounts_service: AccountsService = Depends(get_accounts_service)):
    """
    Get configuration fields required for a specific connector with type information.

    Args:
        connector_name: Name of the connector to get config map for

    Returns:
        Dictionary mapping field names to their type information.
        Each field contains:
        - type: The expected data type (e.g., "str", "SecretStr", "int")
        - required: Whether the field is required
        - allowed_values: List of accepted values (only for Literal fields)
        - prompt: Human readable prompt from the Hummingbot config map (when defined)
    """
    return accounts_service.get_connector_config_map(connector_name)


@router.get("/{connector_name}/capabilities", response_model=ConnectorCapabilities)
async def get_connector_capabilities(request: Request, connector_name: str):
    """Return explicit connector semantics without constructing it or touching its network."""
    market_data_service: MarketDataService = request.app.state.market_data_service
    connector_service = market_data_service.connector_service
    capabilities = connector_service.get_connector_capabilities(connector_name)
    if not connector_service.is_known_connector(connector_name) and not capabilities.declared:
        raise HTTPException(status_code=404, detail=f"Connector '{connector_name}' not found")
    return capabilities


@router.get(
    "/{connector_name}/trading-rules",
    responses={
        207: {"model": TradingRulesPartialResponse},
        400: {"model": CapabilityErrorResponse},
        404: {"model": TradingRulesNotFoundResponse},
    },
)
async def get_trading_rules(
    request: Request,
    connector_name: str,
    trading_pairs: Optional[List[str]] = Query(default=None, description="Filter by specific trading pairs")
):
    """
    Get trading rules for a connector, optionally filtered by trading pairs.

    This endpoint uses the MarketDataService to access non-trading connector instances,
    which means no authentication or account setup is required.

    Args:
        request: FastAPI request object
        connector_name: Name of the connector (e.g., 'binance', 'binance_perpetual')
        trading_pairs: Optional list of trading pairs to filter by (e.g., ['BTC-USDT', 'ETH-USDT'])

    Returns:
        Dictionary mapping trading pairs to their trading rules

    Raises:
        HTTPException: 404 if connector not found, 500 for other errors
    """
    try:
        market_data_service: MarketDataService = request.app.state.market_data_service

        # Get trading rules (filtered by trading pairs if provided)
        rules = await market_data_service.get_trading_rules(connector_name, trading_pairs)

        if "error" in rules:
            raise HTTPException(
                status_code=404,
                detail=TradingRulesNotFoundDetail(
                    message="Trading rules are not available for this connector",
                    connector=connector_name,
                    trading_pairs=trading_pairs or [],
                ).model_dump(),
            )

        valid_rules = {}
        pair_errors = {}
        for trading_pair, rule in rules.items():
            if isinstance(rule, dict) and "error" in rule:
                pair_errors[trading_pair] = TradingRuleError(
                    message=f"Trading pair '{trading_pair}' is not available",
                    trading_pair=trading_pair,
                )
            else:
                valid_rules[trading_pair] = rule

        if pair_errors and not valid_rules:
            raise HTTPException(
                status_code=404,
                detail=TradingRulesNotFoundDetail(
                    message="No trading rules found for the requested pairs",
                    connector=connector_name,
                    trading_pairs=list(pair_errors),
                ).model_dump(),
            )

        if pair_errors:
            partial = TradingRulesPartialResponse(
                connector=connector_name,
                rules=valid_rules,
                errors=pair_errors,
            )
            return JSONResponse(status_code=207, content=partial.model_dump(mode="json"))

        # Preserve the existing all-success mapping for SDK compatibility.
        return valid_rules

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving trading rules: {str(e)}")


@router.get("/{connector_name}/order-types")
async def get_supported_order_types(request: Request, connector_name: str):
    """
    Get order types supported by a specific connector.

    This endpoint uses the MarketDataService to access non-trading connector instances,
    which means no authentication or account setup is required.

    Args:
        request: FastAPI request object
        connector_name: Name of the connector (e.g., 'binance', 'binance_perpetual')

    Returns:
        List of supported order types (LIMIT, MARKET, LIMIT_MAKER)

    Raises:
        HTTPException: 404 if connector not found, 500 for other errors
    """
    try:
        market_data_service: MarketDataService = request.app.state.market_data_service

        # Access connector through UnifiedConnectorService
        # This creates a data connector if it doesn't exist
        try:
            connector_instance = market_data_service.connector_service.get_data_connector(connector_name)
        except (KeyError, ValueError) as e:
            raise HTTPException(status_code=404, detail=f"Connector '{connector_name}' not found: {str(e)}")

        # Get supported order types
        if hasattr(connector_instance, 'supported_order_types'):
            order_types = [order_type.name for order_type in connector_instance.supported_order_types()]
            return {"connector": connector_name, "supported_order_types": order_types}
        else:
            raise HTTPException(status_code=404, detail=f"Connector '{connector_name}' does not support order types query")

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving order types: {str(e)}")
