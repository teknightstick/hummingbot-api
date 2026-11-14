"""
UnifiedConnectorService - Single source of truth for all connector instances.

This service consolidates connector management from:
- ConnectorManager (trading connectors)
- MarketDataProvider._non_trading_connectors (data-only connectors)

Key features:
- Trading connectors: authenticated, per-account, with order tracking
- Data connectors: non-authenticated, shared, for public market data
- get_best_connector_for_market(): prefers trading connector (has order book tracker)
"""
import asyncio
import inspect
import logging
import time
from contextlib import asynccontextmanager
from decimal import Decimal
from types import MappingProxyType
from typing import Any, Dict, List, Mapping, Optional, Set, Tuple

from hummingbot.client.config.config_crypt import ETHKeyFileSecretManger
from hummingbot.client.config.config_helpers import ClientConfigAdapter, api_keys_from_connector_config_map, get_connector_class
from hummingbot.client.settings import AllConnectorSettings
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.connector.connector_metrics_collector import TradeVolumeMetricCollector
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.gateway.gateway import Gateway
from hummingbot.connector.perpetual_derivative_py_base import PerpetualDerivativePyBase
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
from hummingbot.core.utils.async_utils import safe_ensure_future

from models.connectors import CapabilityErrorDetail, ConnectorCapabilities
from services.connector_capabilities import (
    ConnectorCapabilityError,
    ConnectorCapabilityRegistry,
    ConnectorFeature,
)
from utils.file_system import fs_util
from utils.hummingbot_api_config_adapter import HummingbotAPIConfigAdapter
from utils.security import BackendAPISecurity

logger = logging.getLogger(__name__)

_CONNECTOR_CLEANUP_TIMEOUT = 0.05
_TASK_DRAIN_TIMEOUT = 0.01
_INSTANCE_FEATURE_CALLABLE_CHAINS = MappingProxyType(
    {
        ConnectorFeature.ORDER_SUBMISSION.value: ("buy", "sell", "cancel"),
    }
)


class UnknownConnectorError(ValueError):
    """Raised when a connector name is not a known Hummingbot connector.

    Subclasses ValueError so existing ``except ValueError`` handlers keep working.
    """


class UnifiedConnectorService:
    """
    Single source of truth for ALL connector instances.

    Manages two types of connectors:
    1. Trading connectors: authenticated, per-account, with full trading capabilities
    2. Data connectors: non-authenticated, shared, for public market data only

    The key method `get_best_connector_for_market()` ensures that order book
    operations use the trading connector when available (which already has
    order_book_tracker running), falling back to data connector otherwise.
    """

    METRICS_ACTIVATION_INTERVAL = Decimal("900")  # 15 minutes
    METRICS_VALUATION_TOKEN = "USDT"

    def __init__(
        self,
        secrets_manager: ETHKeyFileSecretManger,
        db_manager=None,
        bootstrap_capabilities: Optional[Mapping[str, ConnectorCapabilities]] = None,
    ):
        self.secrets_manager = secrets_manager
        self.db_manager = db_manager

        # Trading connectors: account_name -> connector_name -> ConnectorBase
        self._trading_connectors: Dict[str, Dict[str, ConnectorBase]] = {}

        # Data-only connectors: connector_name -> ConnectorBase (shared, non-authenticated)
        self._data_connectors: Dict[str, ConnectorBase] = {}
        self._data_connectors_started: Dict[str, bool] = {}

        # Order and funding recorders (for trading connectors)
        self._orders_recorders: Dict[str, Any] = {}
        self._funding_recorders: Dict[str, Any] = {}
        self._metrics_collectors: Dict[str, TradeVolumeMetricCollector] = {}

        # Locks to prevent race conditions in connector creation
        self._connector_locks: Dict[Tuple[str, str], asyncio.Lock] = {}
        self._connector_lock_users: Dict[Tuple[str, str], int] = {}
        self._connector_creation_failures: Dict[
            Tuple[str, str], Dict[str, Any]
        ] = {}
        self._connector_locks_guard = asyncio.Lock()
        self._connector_creation_residuals: Dict[Tuple[str, str], Dict[str, Any]] = {}
        self._connector_cleanup_owners: Dict[Tuple[str, str, str], Any] = {}
        self._connector_cleanup_indices: Dict[Tuple[str, str], Dict[str, Any]] = {}
        self._connector_residual_tasks: Dict[
            Tuple[str, str, str], Set[asyncio.Task]
        ] = {}
        self._connector_cleanup_phases: Dict[
            Tuple[str, str, str], Dict[str, Dict[str, Any]]
        ] = {}

        # Connector settings cache
        self._conn_settings = AllConnectorSettings.get_connector_settings()

        # Rate provider for trade-volume telemetry (set to MarketDataService by main on startup).
        # Exposes get_pair_rate(pair) -> Decimal, the RateOracle-compatible interface.
        self._rate_provider = None

        # Bootstrap declarations are copied into an immutable registry at construction.
        # Undeclared connectors preserve their existing generic behavior.
        self._capability_registry = ConnectorCapabilityRegistry(bootstrap_capabilities)

    def set_rate_provider(self, rate_provider):
        """Set the rate provider used by trade-volume telemetry (MarketDataService)."""
        self._rate_provider = rate_provider

    def _get_cached_connector(self, connector_name: str) -> Optional[ConnectorBase]:
        """Resolve an existing instance without constructing or authenticating anything."""
        connector = getattr(self, "_data_connectors", {}).get(connector_name)
        if connector is not None:
            return connector
        for account_connectors in getattr(self, "_trading_connectors", {}).values():
            connector = account_connectors.get(connector_name)
            if connector is not None:
                return connector
        return None

    def get_connector_capabilities(self, connector_name: str, connector: ConnectorBase = None):
        """Return declared semantics without constructing a connector or starting its network."""
        if connector is None:
            connector = self._get_cached_connector(connector_name)
        return self._capability_registry.get(connector_name, connector)

    def require_capability(
        self,
        connector_name: str,
        feature: ConnectorFeature,
        trading_pair: str = None,
        connector: ConnectorBase = None,
    ) -> None:
        """Reject unsupported semantics before connector or tracker lookup."""
        if connector is None:
            connector = self._get_cached_connector(connector_name)
        self._capability_registry.require(connector_name, feature, trading_pair, connector)

    def _validate_instance_capabilities(
        self, connector_name: str, connector: ConnectorBase
    ) -> None:
        """Validate instance-only feature chains before publishing the connector."""
        validated_features = frozenset(
            feature
            for feature, member_names in _INSTANCE_FEATURE_CALLABLE_CHAINS.items()
            if all(
                callable(getattr(connector, member_name, None))
                for member_name in member_names
            )
        )
        self._capability_registry.validate_instance(
            connector_name,
            connector,
            validated_features=validated_features,
        )

    def _is_perpetual_connector(self, connector: ConnectorBase) -> bool:
        """Check if connector is a perpetual derivative connector.

        Args:
            connector: The connector instance to check

        Returns:
            True if perpetual connector, False otherwise
        """
        return isinstance(connector, PerpetualDerivativePyBase)

    def connector_supports_perpetual(
        self, connector_name: str, connector: ConnectorBase = None
    ) -> bool:
        """Return declared semantics first, then preserve the legacy type check."""
        capabilities = self.get_connector_capabilities(connector_name, connector)
        if capabilities.declared and capabilities.perpetual is not None:
            return capabilities.perpetual
        if connector is not None:
            return self._is_perpetual_connector(connector)
        return connector_name.endswith("_perpetual")

    def is_gateway_connector(self, connector: ConnectorBase) -> bool:
        """Check if connector is a unified Gateway (on-chain) connector.

        Args:
            connector: The connector instance to check

        Returns:
            True if Gateway connector, False otherwise
        """
        return isinstance(connector, Gateway)

    # =========================================================================
    # Trading Connector Management (authenticated, per-account)
    # =========================================================================

    @staticmethod
    def _connector_key(account_name: str, connector_name: str) -> Tuple[str, str]:
        return account_name, connector_name

    @staticmethod
    def _index_key(account_name: str, connector_name: str) -> str:
        return f"{account_name}:{connector_name}"

    @staticmethod
    def _owner_key(
        account_name: str, connector_name: str, owner_kind: str
    ) -> Tuple[str, str, str]:
        return account_name, connector_name, owner_kind

    def _ensure_connector_cleanup_state(self) -> None:
        """Initialize private lifecycle registries for dependency-light instances."""
        defaults = (
            ("_connector_creation_residuals", {}),
            ("_connector_creation_failures", {}),
            ("_connector_cleanup_owners", {}),
            ("_connector_cleanup_indices", {}),
            ("_connector_residual_tasks", {}),
            ("_connector_cleanup_phases", {}),
        )
        for name, value in defaults:
            if not hasattr(self, name):
                setattr(self, name, value)

    async def get_trading_connector(
        self,
        account_name: str,
        connector_name: str
    ) -> ConnectorBase:
        """
        Get or create an authenticated trading connector for a specific account.

        Trading connectors have:
        - API key authentication
        - Order tracking (OrdersRecorder)
        - Funding tracking for perpetuals (FundingRecorder)
        - Metrics collection
        - Full trading capabilities

        Args:
            account_name: The account name
            connector_name: The connector name (e.g., "binance", "binance_perpetual")

        Returns:
            Initialized trading connector
        """
        connector_key = self._connector_key(account_name, connector_name)

        async with self._connector_creation_lock(connector_key):
            self._ensure_connector_cleanup_state()
            residual = self._connector_creation_residuals.get(connector_key)
            if residual is not None:
                raise ConnectorCapabilityError(
                    CapabilityErrorDetail.model_validate(residual)
                )
            failure = self._connector_creation_failures.get(connector_key)
            if failure is not None:
                raise ConnectorCapabilityError(
                    CapabilityErrorDetail.model_validate(failure)
                )
            existing = self._trading_connectors.get(account_name, {}).get(connector_name)
            if existing is not None:
                return existing

            try:
                connector = await self._create_and_initialize_trading_connector(
                    account_name, connector_name
                )
            except ConnectorCapabilityError as error:
                # Callers already queued on this creation attempt must observe its
                # typed terminal result instead of constructing and stopping the
                # same failed connector again. The lock prunes this transient
                # result when the final waiter leaves, so a later call may retry.
                self._connector_creation_failures[connector_key] = dict(error.detail)
                raise
            self._trading_connectors.setdefault(account_name, {})[connector_name] = connector
            self._connector_creation_residuals.pop(connector_key, None)
            return connector

    @asynccontextmanager
    async def _connector_creation_lock(self, connector_key: Tuple[str, str]):
        """Serialize creation and prune the caller-controlled cache key after use."""
        async with self._connector_locks_guard:
            lock = self._connector_locks.setdefault(connector_key, asyncio.Lock())
            self._connector_lock_users[connector_key] = self._connector_lock_users.get(connector_key, 0) + 1
        acquired = False
        try:
            await lock.acquire()
            acquired = True
            yield
        finally:
            if acquired:
                lock.release()
            try:
                # Preserve cancellation queued after the transaction releases
                # its connector lock while keeping lock-index pruning atomic.
                await asyncio.sleep(0)
            finally:
                users = self._connector_lock_users.get(connector_key, 1) - 1
                if users:
                    self._connector_lock_users[connector_key] = users
                else:
                    self._connector_lock_users.pop(connector_key, None)
                    if self._connector_locks.get(connector_key) is lock:
                        self._connector_locks.pop(connector_key, None)
                    getattr(self, "_connector_creation_failures", {}).pop(
                        connector_key, None
                    )

    def get_all_trading_connectors(self) -> Dict[str, Dict[str, ConnectorBase]]:
        """
        Get all trading connectors organized by account.

        Returns:
            Dict mapping account_name -> connector_name -> ConnectorBase
        """
        return self._trading_connectors

    def get_account_connectors(self, account_name: str) -> Dict[str, ConnectorBase]:
        """
        Get all connectors for a specific account.

        Args:
            account_name: Account name

        Returns:
            Dict mapping connector_name -> ConnectorBase for this account
        """
        return self._trading_connectors.get(account_name, {})

    def is_trading_connector_initialized(
        self,
        account_name: str,
        connector_name: str
    ) -> bool:
        """Check if a trading connector is already initialized."""
        return (
            account_name in self._trading_connectors and
            connector_name in self._trading_connectors[account_name]
        )

    # =========================================================================
    # Data Connector Management (non-authenticated, shared)
    # =========================================================================

    def is_known_connector(self, connector_name: str) -> bool:
        """True if the name is a known Hummingbot exchange connector.

        Gateway network connectors (e.g. 'solana-mainnet-beta') are not in AllConnectorSettings
        and therefore return False.
        """
        return connector_name in self._conn_settings

    def get_data_connector(self, connector_name: str) -> ConnectorBase:
        """
        Get or create a non-authenticated data connector for public market data.

        Data connectors:
        - No API keys required (public endpoints only)
        - Shared across accounts
        - Used for: trading rules, prices, order books, candles
        - NOT used for: trading, balance queries

        Args:
            connector_name: The connector name

        Returns:
            Non-authenticated connector instance
        """
        if connector_name not in self._data_connectors:
            connector = self._create_data_connector(connector_name)
            self._validate_instance_capabilities(connector_name, connector)
            self._data_connectors[connector_name] = connector
        return self._data_connectors[connector_name]

    async def ensure_data_connector_started(
        self,
        connector_name: str,
        trading_pair: str
    ) -> bool:
        """
        Ensure a data connector's network is started with at least one trading pair.

        This is needed because exchanges close WebSocket connections without subscriptions.

        Args:
            connector_name: The connector name
            trading_pair: Initial trading pair to subscribe to

        Returns:
            True if started successfully
        """
        if self._data_connectors_started.get(connector_name, False):
            return True

        connector = self.get_data_connector(connector_name)

        try:
            # Add trading pair before starting network
            if trading_pair not in connector._trading_pairs:
                connector._trading_pairs.append(trading_pair)

            # Start network
            await connector.start_network()
            self._data_connectors_started[connector_name] = True
            logger.info(f"Started data connector: {connector_name} with pair {trading_pair}")

            # Wait for order book tracker to be ready
            max_wait = 30
            waited = 0
            tracker = connector.order_book_tracker
            while waited < max_wait:
                if tracker._order_book_stream_listener_task is not None:
                    await asyncio.sleep(2.0)
                    break
                await asyncio.sleep(0.5)
                waited += 0.5

            return True

        except Exception as e:
            logger.error(f"Error starting data connector {connector_name}: {e}")
            self._purge_trading_pair_registration(connector, trading_pair)
            return False

    async def _is_trading_pair_supported(
        self,
        connector: ConnectorBase,
        trading_pair: str
    ) -> bool:
        """Check that a trading pair exists in the connector's symbol map.

        Registering an unknown pair poisons the order book WebSocket loop for every
        other pair: the data source iterates over all registered pairs when
        subscribing, so a single unknown pair raises and aborts the whole
        subscription, which then retries and fails forever.
        """
        if not hasattr(connector, "exchange_symbol_associated_to_pair"):
            return True
        try:
            await connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
            return True
        except KeyError:
            return False
        except Exception as e:
            # Symbol map unavailable (network error, etc.) - don't block on it
            logger.warning(f"Could not validate {trading_pair} against symbol map: {e}")
            return True

    def _purge_trading_pair_registration(
        self,
        connector: ConnectorBase,
        trading_pair: str
    ):
        """Unregister a trading pair from the connector and its order book tracker.

        Used to roll back a failed registration so a pair that never got an order
        book cannot break the shared WebSocket subscription.
        """
        targets = [getattr(connector, "_trading_pairs", None)]
        tracker = getattr(connector, "order_book_tracker", None)
        if tracker is not None:
            targets.append(getattr(tracker, "_trading_pairs", None))

        for pairs in targets:
            # Connector and tracker often share the same list object
            if pairs is not None and trading_pair in pairs:
                pairs.remove(trading_pair)
                logger.info(f"Unregistered {trading_pair} after failed order book initialization")

    # =========================================================================
    # Best Connector Selection (THE KEY FIX)
    # =========================================================================

    def get_best_connector_for_market(
        self,
        connector_name: str,
        account_name: Optional[str] = None
    ) -> Optional[ConnectorBase]:
        """
        Get the best available connector for market operations (order books, prices).

        CRITICAL: This method ensures order book initialization uses the correct
        connector. It prefers trading connectors because they already have
        order_book_tracker running with WebSocket connections.

        Resolution:
        1. If an account is supplied, only that account's trading connector
        2. Otherwise, any trading connector for this connector_name
        3. Otherwise, the shared data connector

        Args:
            connector_name: The connector name
            account_name: Optional strict account boundary

        Returns:
            Best available connector for market operations
        """
        # An explicit account is an authority boundary, not a preference. Falling
        # through here can read or mutate another account's connector and tracker.
        if account_name is not None:
            trading = self._trading_connectors.get(account_name, {}).get(connector_name)
            if trading:
                logger.debug(
                    f"Using trading connector for {connector_name} "
                    f"(account: {account_name})"
                )
            return trading

        # Unscoped callers may use any already-running trading connector.
        for acc_name, acc_connectors in self._trading_connectors.items():
            if connector_name in acc_connectors:
                logger.debug(
                    f"Using trading connector for {connector_name} "
                    f"(found in account: {acc_name})"
                )
                return acc_connectors[connector_name]

        # 3. Fall back to data connector
        logger.debug(f"Using data connector for {connector_name} (no trading connector)")
        return self.get_data_connector(connector_name)

    def _resolve_restart_connector_owner(
        self,
        connector_name: str,
        account_name: Optional[str],
    ) -> Tuple[Optional[str], Optional[ConnectorBase], Optional[str]]:
        """Resolve a restart's physical connector and lock owner without guessing."""
        if account_name is not None:
            connector = self._trading_connectors.get(account_name, {}).get(
                connector_name
            )
            if connector is None:
                # Dependency-light callers may override the strict resolver. The
                # production implementation preserves the same account boundary.
                connector = self.get_best_connector_for_market(
                    connector_name, account_name
                )
            return account_name, connector, None

        matches = [
            (owner_account, connectors[connector_name])
            for owner_account, connectors in self._trading_connectors.items()
            if connector_name in connectors
        ]
        if len(matches) > 1:
            return None, None, "ambiguous_connector_owner"
        if matches:
            owner_account, connector = matches[0]
            return owner_account, connector, None

        connector = self._data_connectors.get(connector_name)
        if connector is None:
            connector = self.get_best_connector_for_market(connector_name)
        return "__data__", connector, None

    def validate_order_book_ready(
        self,
        connector_name: str,
        trading_pair: str,
        account_name: Optional[str] = None,
    ) -> bool:
        """Read existing feed state without constructing or mutating a connector."""
        if account_name is not None:
            connector = self._trading_connectors.get(account_name, {}).get(
                connector_name
            )
        else:
            matches = [
                connectors[connector_name]
                for connectors in self._trading_connectors.values()
                if connector_name in connectors
            ]
            if len(matches) > 1:
                return False
            connector = matches[0] if matches else self._data_connectors.get(
                connector_name
            )
        tracker = getattr(connector, "order_book_tracker", None)
        if tracker is None:
            return False
        order_books = getattr(tracker, "order_books", {})
        if trading_pair not in order_books:
            return False
        for owner in (connector, tracker):
            pairs = getattr(owner, "_trading_pairs", None)
            if pairs is not None and trading_pair not in pairs:
                return False
        return True

    # =========================================================================
    # Order Book Initialization
    # =========================================================================

    async def initialize_order_book(
        self,
        connector_name: str,
        trading_pair: str,
        account_name: Optional[str] = None,
        timeout: float = 30.0
    ) -> bool:
        """
        Initialize order book for a trading pair using the best available connector.

        This method:
        1. Gets the best connector (prefers trading over data)
        2. Adds trading pair to order book tracker
        3. Waits for order book to have valid data

        Args:
            connector_name: The connector name
            trading_pair: The trading pair
            account_name: Optional account to prefer
            timeout: Timeout in seconds

        Returns:
            True if order book initialized successfully
        """
        self.require_capability(
            connector_name, ConnectorFeature.DYNAMIC_SUBSCRIPTION, trading_pair
        )
        self.require_capability(
            connector_name, ConnectorFeature.LIVE_ORDER_BOOK, trading_pair
        )
        if account_name is not None:
            # Account-scoped cleanup must never fall back to a connector owned by
            # another account (or to a shared data connector).
            connector = self._trading_connectors.get(account_name, {}).get(
                connector_name
            )
        else:
            connector = self.get_best_connector_for_market(connector_name)

        if not connector:
            logger.error(f"No connector available for {connector_name}")
            return False

        # Gateway/AMM connectors don't have order book trackers - skip initialization
        if not hasattr(connector, 'order_book_tracker') or connector.order_book_tracker is None:
            logger.info(f"Connector {connector_name} doesn't have order book tracker (AMM/Gateway) - skipping")
            return True

        tracker = connector.order_book_tracker

        # Check if already initialized
        if trading_pair in tracker.order_books:
            ob = tracker.order_books[trading_pair]
            try:
                bids, asks = ob.snapshot
                if len(bids) > 0 and len(asks) > 0:
                    logger.info(f"Order book for {trading_pair} already initialized")
                    return True
            except Exception:
                pass

        # Reject pairs the exchange doesn't list before registering them anywhere
        if not await self._is_trading_pair_supported(connector, trading_pair):
            logger.error(
                f"Trading pair {trading_pair} is not listed on {connector_name} - "
                f"refusing to register it (check the quote asset)"
            )
            return False

        # Lazy symbol/instrument discovery may replace api_capabilities. Validate
        # that loaded declaration before any pair, book, or tracker state changes.
        self._validate_instance_capabilities(connector_name, connector)

        # For data connectors, ensure network is started
        if connector_name in self._data_connectors:
            if not self._data_connectors_started.get(connector_name, False):
                success = await self.ensure_data_connector_started(
                    connector_name, trading_pair
                )
                if not success:
                    return False
                # Wait for order book after starting
                return await self._wait_for_order_book(tracker, trading_pair, timeout)
            else:
                # Connector started, dynamically add trading pair
                success = await self._add_trading_pair_to_tracker(
                    connector, trading_pair
                )
                if not success:
                    return False

        # For trading connectors, dynamically add trading pair
        else:
            success = await self._add_trading_pair_to_tracker(connector, trading_pair)
            if not success:
                return False

        # Wait for order book to have data
        return await self._wait_for_order_book(tracker, trading_pair, timeout)

    def _is_tracker_running(self, tracker) -> bool:
        """Check if the order book tracker is running."""
        if not tracker:
            return False
        task = tracker._order_book_stream_listener_task
        if task and not task.done():
            return True
        task = tracker._init_order_books_task
        if task and not task.done():
            return True
        return False

    async def _add_trading_pair_to_tracker(
        self,
        connector: ExchangePyBase,
        trading_pair: str
    ) -> bool:
        """Add a trading pair to connector's order book tracker.

        ExchangePyBase connectors have:
        - order_book_tracker with _trading_pairs, start(), _orderbook_ds
        - add_trading_pair() for dynamic addition

        Approach:
        1. If tracker is running, use connector.add_trading_pair()
        2. Otherwise, register the pair and start the tracker
        """
        try:
            # Safety check - gateway/AMM connectors don't have order book trackers
            if not hasattr(connector, 'order_book_tracker') or connector.order_book_tracker is None:
                logger.debug(f"Connector {type(connector).__name__} doesn't have order book tracker")
                return True

            tracker = connector.order_book_tracker

            # Case 1: Tracker is already running and ready
            if self._is_tracker_running(tracker) and tracker.ready:
                if trading_pair in tracker.order_books:
                    logger.debug(f"Order book for {trading_pair} already exists")
                    return True

                logger.info(f"Adding {trading_pair} to running tracker")
                result = await connector.add_trading_pair(trading_pair)
                if result:
                    logger.info(f"Successfully added {trading_pair}")
                    return True
                logger.warning(f"add_trading_pair() returned False for {trading_pair}")

            # Case 2: Tracker not running - start it with this trading pair
            else:
                logger.info(f"Starting order book tracker for {type(connector).__name__} with {trading_pair}")

                # Register the trading pair before starting tracker
                if trading_pair not in tracker._trading_pairs:
                    tracker._trading_pairs.append(trading_pair)

                tracker.start()
                try:
                    await asyncio.wait_for(tracker.wait_ready(), timeout=30.0)
                    logger.info(f"Order book tracker ready for {type(connector).__name__}")
                except asyncio.TimeoutError:
                    logger.warning("Timeout waiting for tracker to be ready")

                if trading_pair in tracker.order_books:
                    logger.info(f"Order book for {trading_pair} initialized")
                    return True

            # Fallback: Get order book snapshot directly via REST
            logger.info(f"Fallback order book initialization for {trading_pair}")
            try:
                order_book = await connector._orderbook_ds.get_new_order_book(trading_pair)
                tracker.order_books[trading_pair] = order_book
                if trading_pair not in tracker._trading_pairs:
                    tracker._trading_pairs.append(trading_pair)
                logger.info(f"Initialized order book for {trading_pair} via REST fallback")
                return True
            except Exception as e:
                logger.error(f"Fallback order book initialization failed: {e}")

            logger.error(f"Failed to add {trading_pair} to order book tracker")
            self._purge_trading_pair_registration(connector, trading_pair)
            return False

        except Exception as e:
            logger.error(f"Error adding trading pair {trading_pair}: {e}", exc_info=True)
            self._purge_trading_pair_registration(connector, trading_pair)
            return False

    async def remove_trading_pair(
        self,
        connector_name: str,
        trading_pair: str,
        account_name: Optional[str] = None
    ) -> bool:
        """
        Remove a trading pair from a connector's order book tracker.

        This method cleans up order book resources for a trading pair that is
        no longer needed. Useful for:
        - Executor cleanup when stopping
        - Memory management for unused pairs
        - Account cleanup operations

        Args:
            connector_name: The connector name
            trading_pair: The trading pair to remove
            account_name: Optional account to target specific trading connector

        Returns:
            True if successfully removed, False otherwise
        """
        self.require_capability(
            connector_name, ConnectorFeature.DYNAMIC_SUBSCRIPTION, trading_pair
        )
        if account_name is not None:
            # Never clean another account's connector when the requested account
            # does not own this connector.
            connector = self._trading_connectors.get(account_name, {}).get(
                connector_name
            )
        else:
            connector = self.get_best_connector_for_market(connector_name)

        if not connector:
            logger.warning(f"No connector available for {connector_name} to remove {trading_pair}")
            return False

        return await self._remove_trading_pair_from_tracker(connector, trading_pair)

    async def _remove_trading_pair_from_tracker(
        self,
        connector: ExchangePyBase,
        trading_pair: str
    ) -> bool:
        """Remove a trading pair from connector's order book tracker.

        ExchangePyBase.remove_trading_pair() handles:
        - Order book cleanup via order_book_tracker
        - Funding info cleanup for perpetual connectors
        """
        try:
            result = await connector.remove_trading_pair(trading_pair)
            if result:
                logger.info(f"Removed trading pair {trading_pair}")
                return True

            # Fallback: Manual removal from tracker (if connector has one)
            if not hasattr(connector, 'order_book_tracker') or connector.order_book_tracker is None:
                return True  # No tracker to clean up for AMM/Gateway connectors
            tracker = connector.order_book_tracker
            if trading_pair in tracker.order_books:
                del tracker.order_books[trading_pair]
                self._purge_trading_pair_registration(connector, trading_pair)
                logger.info(f"Removed trading pair {trading_pair} via manual fallback")
                return True

            # No order book, but the pair may still be registered - a pair stuck in
            # the subscription list with no order book is exactly what breaks the
            # WebSocket loop, so unregister it anyway.
            if trading_pair in getattr(tracker, "_trading_pairs", []):
                self._purge_trading_pair_registration(connector, trading_pair)
                logger.info(f"Unregistered untracked trading pair {trading_pair}")
                return True

            logger.warning(f"Trading pair {trading_pair} not found")
            return False

        except Exception as e:
            logger.error(f"Error removing trading pair {trading_pair}: {e}")
            return False

    async def _wait_for_websocket_ready(
        self,
        connector: ExchangePyBase,
        timeout: float = 10.0
    ) -> bool:
        """Wait for the order book data source WebSocket to be connected."""
        data_source = connector._orderbook_ds
        waited = 0
        interval = 0.2

        while waited < timeout:
            if data_source._ws_assistant is not None:
                logger.debug(f"WebSocket ready for {type(connector).__name__}")
                return True
            await asyncio.sleep(interval)
            waited += interval

        logger.warning(f"Timeout waiting for WebSocket connection on {type(connector).__name__}")
        return False

    async def _wait_for_order_book(
        self,
        tracker,
        trading_pair: str,
        timeout: float
    ) -> bool:
        """Wait for order book to have valid bid/ask data."""
        waited = 0
        interval = 0.5

        while waited < timeout:
            if trading_pair in tracker.order_books:
                ob = tracker.order_books[trading_pair]
                try:
                    bids, asks = ob.snapshot
                    if len(bids) > 0 and len(asks) > 0:
                        logger.info(
                            f"Order book for {trading_pair} ready with "
                            f"{len(bids)} bids and {len(asks)} asks"
                        )
                        return True
                except Exception:
                    pass
            await asyncio.sleep(interval)
            waited += interval

        logger.warning(f"Timeout waiting for {trading_pair} order book")
        return False

    # =========================================================================
    # Trading Connector Creation (internal)
    # =========================================================================

    async def _create_and_initialize_trading_connector(
        self,
        account_name: str,
        connector_name: str
    ) -> ConnectorBase:
        """Create a connector transactionally and clean every owned index on failure."""
        cache_key = self._index_key(account_name, connector_name)
        missing = object()
        baselines = {
            "connector": self._trading_connectors.get(account_name, {}).get(
                connector_name, missing
            ),
            "orders": self._orders_recorders.get(cache_key, missing),
            "funding": self._funding_recorders.get(cache_key, missing),
            "metrics": self._metrics_collectors.get(cache_key, missing),
        }
        creation_owner: Dict[str, Any] = {}
        try:
            return await self._create_and_initialize_trading_connector_unchecked(
                account_name, connector_name, creation_owner
            )
        except BaseException as primary_error:
            residual = await self._complete_failed_connector_creation(
                account_name,
                connector_name,
                creation_owner.get("connector"),
                cache_key,
                baselines,
                missing,
            )
            if isinstance(primary_error, asyncio.CancelledError):
                raise
            if residual is not None:
                raise ConnectorCapabilityError(residual) from primary_error
            raise

    async def _create_and_initialize_trading_connector_unchecked(
        self,
        account_name: str,
        connector_name: str,
        creation_owner: Dict[str, Any],
    ) -> ConnectorBase:
        """Create and fully initialize a trading connector."""
        # Authenticate and create connector
        connector = self._create_trading_connector(account_name, connector_name)
        creation_owner["connector"] = connector
        self._validate_instance_capabilities(connector_name, connector)
        setattr(connector, "_api_cleanup_account", account_name)
        setattr(connector, "_api_cleanup_connector", connector_name)

        # Initialize symbol map and trading rules
        await connector._initialize_trading_pair_symbol_map()
        # Manzana replaces api_capabilities with pair-narrowed loaded truth at
        # this boundary. Reject incompatible truth before later initialization
        # creates recorders, metrics, tasks, or other externally visible state.
        self._validate_instance_capabilities(connector_name, connector)
        await connector._update_trading_rules()
        await connector._update_balances()

        # Perpetual-specific setup
        if self._is_perpetual_connector(connector):
            if PositionMode.HEDGE in connector.supported_position_modes():
                connector.set_position_mode(PositionMode.HEDGE)
            await connector._update_positions()

        # Load existing orders from database
        if self.db_manager:
            await self._load_existing_orders(connector, account_name, connector_name)

        # Setup order and funding recorders
        cache_key = f"{account_name}:{connector_name}"
        if self.db_manager and cache_key not in self._orders_recorders:
            from services.orders_recorder import OrdersRecorder
            orders_recorder = OrdersRecorder(self.db_manager, account_name, connector_name)
            orders_recorder.start(connector)
            self._orders_recorders[cache_key] = orders_recorder

            if self._is_perpetual_connector(connector):
                from services.funding_recorder import FundingRecorder
                funding_recorder = FundingRecorder(self.db_manager, account_name, connector_name)
                funding_recorder.start(connector)
                self._funding_recorders[cache_key] = funding_recorder

        # Initialize metrics
        self._initialize_metrics(connector, account_name, connector_name, cache_key)

        # Start network tasks
        await self._start_connector_network(connector)

        # Only update order status for orders loaded from DB (balances, rules, positions
        # were already fetched above — no need to repeat via _update_connector_state)
        if connector.in_flight_orders:
            try:
                connector._set_current_timestamp(time.time())
                await connector._update_order_status()
            except Exception as e:
                logger.error(f"Error updating initial order status for {connector_name}: {e}")

        # This is the final declaration boundary. Any accepted initialization
        # seam that replaces api_capabilities after symbol discovery is still
        # rejected before the caller can publish the connector in its cache.
        self._validate_instance_capabilities(connector_name, connector)
        logger.info(f"Initialized trading connector {connector_name} for {account_name}")
        return connector

    @property
    def connector_creation_residuals(self) -> Dict[str, Dict[str, Any]]:
        """Expose failed-creation cleanup state without exposing the mutable ledger."""
        return {
            (
                self._index_key(*key)
                if isinstance(key, tuple) and len(key) >= 2
                else str(key)
            ): dict(value)
            for key, value in getattr(self, "_connector_creation_residuals", {}).items()
        }

    async def _drain_owner_tasks(
        self,
        owner_key: Tuple[str, str, str],
        tasks: Optional[Set[asyncio.Task]] = None,
    ) -> bool:
        """Cancel and consume retained owner tasks under one hard deadline."""
        self._ensure_connector_cleanup_state()
        current = asyncio.current_task()
        new_tasks = set(tasks or ())
        retained_tasks = self._connector_residual_tasks.pop(owner_key, set())
        owned = new_tasks | retained_tasks
        pending = {task for task in owned if task is not current and not task.done()}
        # Retained tasks already received cancellation. Retrying waits for their
        # terminal state without using repeated cancellation as a false-success
        # mechanism against cancellation-suppressing cleanup code.
        for task in pending & new_tasks:
            if task.cancelling() == 0:
                task.cancel()
        if pending:
            done, pending = await asyncio.wait(
                pending, timeout=_TASK_DRAIN_TIMEOUT
            )
            for task in done:
                try:
                    task.exception()
                except (asyncio.CancelledError, Exception):
                    pass
        if pending:
            self._connector_residual_tasks[owner_key] = set(pending)
            return False
        for task in owned:
            if task.done():
                try:
                    task.exception()
                except (asyncio.CancelledError, Exception):
                    pass
        return True

    async def _run_owner_operation(
        self,
        owner_key: Tuple[str, str, str],
        phase_name: str,
        operation,
        *,
        timeout: float = _CONNECTOR_CLEANUP_TIMEOUT,
    ):
        """Run one non-repeatable cleanup phase and retain its exact task/result."""
        self._ensure_connector_cleanup_state()
        owner_phases = self._connector_cleanup_phases.setdefault(owner_key, {})
        phase = owner_phases.setdefault(
            phase_name,
            {
                "task": None,
                "complete": False,
                "result": None,
                "error_code": None,
                "cancel_requested": False,
            },
        )
        if phase["complete"]:
            if phase["error_code"] is not None:
                raise RuntimeError(phase["error_code"])
            return phase["result"]

        task = phase["task"]
        if task is None:
            try:
                result = operation()
            except BaseException as exc:
                if isinstance(exc, asyncio.CancelledError):
                    raise
                phase["complete"] = True
                phase["error_code"] = f"{phase_name}_raised"
                logger.error(
                    "Cleanup phase %s failed for %s/%s (%s)",
                    phase_name,
                    owner_key[0],
                    owner_key[1],
                    type(exc).__name__,
                )
                raise RuntimeError(phase["error_code"]) from None
            if not inspect.isawaitable(result):
                phase["complete"] = True
                phase["result"] = result
                if result is False:
                    phase["error_code"] = f"{phase_name}_returned_false"
                    raise RuntimeError(phase["error_code"])
                return result
            task = asyncio.ensure_future(result)
            phase["task"] = task
            self._connector_residual_tasks.setdefault(owner_key, set()).add(task)

        if not task.done():
            try:
                _, pending = await asyncio.wait({task}, timeout=timeout)
            except asyncio.CancelledError:
                self._connector_residual_tasks.setdefault(owner_key, set()).add(task)
                raise
            if pending:
                if not phase["cancel_requested"] and task.cancelling() == 0:
                    task.cancel()
                    phase["cancel_requested"] = True
                self._connector_residual_tasks.setdefault(owner_key, set()).add(task)
                raise RuntimeError(f"{phase_name}_pending")

        retained = self._connector_residual_tasks.get(owner_key)
        if retained is not None:
            retained.discard(task)
            if not retained:
                self._connector_residual_tasks.pop(owner_key, None)
        try:
            result = task.result()
        except asyncio.CancelledError:
            phase["complete"] = True
            phase["error_code"] = f"{phase_name}_cancelled"
            raise RuntimeError(phase["error_code"]) from None
        except Exception as exc:
            phase["complete"] = True
            phase["error_code"] = f"{phase_name}_raised"
            logger.error(
                "Cleanup phase %s failed for %s/%s (%s)",
                phase_name,
                owner_key[0],
                owner_key[1],
                type(exc).__name__,
            )
            raise RuntimeError(phase["error_code"]) from None
        phase["complete"] = True
        phase["result"] = result
        if result is False:
            phase["error_code"] = f"{phase_name}_returned_false"
            raise RuntimeError(phase["error_code"])
        return result

    def _finish_owner_cleanup(self, owner_key: Tuple[str, str, str]) -> None:
        """Discard phase truth only after the complete owner transaction succeeds."""
        self._connector_cleanup_phases.pop(owner_key, None)
        self._connector_residual_tasks.pop(owner_key, None)
        self._connector_cleanup_owners.pop(owner_key, None)

    async def _stop_owned_index(
        self,
        account_name: str,
        connector_name: str,
        owner_kind: str,
        value,
    ) -> None:
        self._ensure_connector_cleanup_state()
        owner_key = self._owner_key(account_name, connector_name, owner_kind)
        self._connector_cleanup_owners[owner_key] = value
        if not await self._drain_owner_tasks(owner_key):
            raise RuntimeError("index_cleanup_pending")
        stop = getattr(value, "stop", None)
        if stop is None:
            self._finish_owner_cleanup(owner_key)
            return
        await self._run_owner_operation(owner_key, "index_stop", stop)
        if not await self._drain_owner_tasks(owner_key):
            raise RuntimeError("index_cleanup_pending")
        self._finish_owner_cleanup(owner_key)

    async def _finalize_failed_connector_creation(
        self,
        account_name: str,
        connector_name: str,
        connector,
        cache_key: str,
        baselines: Dict[str, Any],
        missing,
    ) -> Optional[CapabilityErrorDetail]:
        """Undo only indices and tasks owned by the failed creation transaction."""
        self._ensure_connector_cleanup_state()
        connector_key = self._connector_key(account_name, connector_name)
        cleanup_codes = []
        retained_indices = self._connector_cleanup_indices
        failed_indices: Dict[str, Any] = {}
        account_connectors = self._trading_connectors.get(account_name)
        current_connector = (
            account_connectors.get(connector_name, missing)
            if account_connectors is not None
            else missing
        )
        baseline_connector = baselines["connector"]
        owned_connector = connector
        if (
            owned_connector is None
            and current_connector is not missing
            and current_connector is not baseline_connector
        ):
            owned_connector = current_connector
        if owned_connector is not None:
            try:
                await self._stop_connector_network(
                    owned_connector, account_name, connector_name
                )
            except BaseException:
                cleanup_codes.append("connector_cleanup_incomplete")

        indices = (
            ("orders", self._orders_recorders),
            ("funding", self._funding_recorders),
            ("metrics", self._metrics_collectors),
        )
        for label, mapping in indices:
            baseline = baselines[label]
            current = mapping.get(cache_key, missing)
            if current is not missing and current is not baseline:
                try:
                    await self._stop_owned_index(
                        account_name, connector_name, label, current
                    )
                except BaseException:
                    cleanup_codes.append(f"{label}_cleanup_incomplete")
                    failed_indices[label] = current
            if baseline is missing:
                mapping.pop(cache_key, None)
            else:
                mapping[cache_key] = baseline

        account_connectors = self._trading_connectors.get(account_name)
        if account_connectors is not None:
            if baseline_connector is missing:
                account_connectors.pop(connector_name, None)
            else:
                account_connectors[connector_name] = baseline_connector
            if not account_connectors:
                self._trading_connectors.pop(account_name, None)

        residuals = self._connector_creation_residuals
        if not cleanup_codes:
            residuals.pop(connector_key, None)
            retained_indices.pop(connector_key, None)
            return None
        if failed_indices:
            retained_indices[connector_key] = failed_indices
        detail = CapabilityErrorDetail(
            code="connector_creation_cleanup_residual",
            message=f"Connector creation cleanup is incomplete for '{cache_key}'",
            connector=connector_name,
            feature="connector_creation",
            reason="cleanup_incomplete",
            cleanup_scope="connector",
            cleanup_code=cleanup_codes[0],
        )
        residuals[connector_key] = detail.model_dump(exclude_none=True)
        return detail

    async def _complete_failed_connector_creation(
        self,
        account_name: str,
        connector_name: str,
        connector,
        cache_key: str,
        baselines: Dict[str, Any],
        missing,
    ) -> Optional[CapabilityErrorDetail]:
        """Finish bounded cleanup even while preserving primary caller cancellation."""
        cleanup_task = asyncio.create_task(
            self._finalize_failed_connector_creation(
                account_name,
                connector_name,
                connector,
                cache_key,
                baselines,
                missing,
            )
        )
        cancelled = False
        while not cleanup_task.done():
            try:
                await asyncio.shield(cleanup_task)
            except asyncio.CancelledError:
                cancelled = True
                continue
        result = cleanup_task.result()
        if cancelled:
            raise asyncio.CancelledError()
        return result

    def _create_trading_connector(
        self,
        account_name: str,
        connector_name: str
    ) -> ConnectorBase:
        """Create a trading connector with API keys.

        For Gateway network connectors (e.g., 'solana-mainnet-beta'), creates a unified
        Gateway connector which auto-detects chain/network and uses the default wallet.
        The dex_name and trading_type are passed to methods, not to the connector.
        """
        BackendAPISecurity.login_account(
            account_name=account_name,
            secrets_manager=self.secrets_manager
        )

        # Check if this is a Gateway network connector
        # Gateway connectors are NOT in AllConnectorSettings (those are exchange connectors)
        # Network format: "chain-network" (e.g., "solana-mainnet-beta", "ethereum-mainnet")
        if connector_name not in self._conn_settings:
            logger.info(f"Creating Gateway connector for network: {connector_name}")
            return Gateway(
                connector_name=connector_name,
                trading_pairs=[],
                trading_required=True,
            )

        conn_setting = self._conn_settings[connector_name]
        keys = BackendAPISecurity.api_keys(connector_name)

        init_params = conn_setting.conn_init_parameters(
            trading_pairs=[],
            trading_required=True,
            api_keys=keys,
        )

        connector_class = get_connector_class(connector_name)
        return connector_class(**init_params)

    def _create_data_connector(self, connector_name: str) -> ConnectorBase:
        """Create a non-authenticated data connector."""
        conn_setting = self._conn_settings.get(connector_name)
        if not conn_setting:
            raise UnknownConnectorError(f"Connector {connector_name} not found")

        # Get config keys but don't use real API keys
        connector_config = AllConnectorSettings.get_connector_config_keys(connector_name)
        if getattr(connector_config, "use_auth_for_public_endpoints", False):
            api_keys = api_keys_from_connector_config_map(
                ClientConfigAdapter(connector_config)
            )
        elif connector_config is not None:
            api_keys = {
                key: ""
                for key in connector_config.__class__.model_fields.keys()
                if key != "connector"
            }
        else:
            api_keys = {}

        init_params = conn_setting.conn_init_parameters(
            trading_pairs=[],
            trading_required=False,
            api_keys=api_keys,
        )

        connector_class = get_connector_class(connector_name)
        connector = connector_class(**init_params)

        logger.info(f"Created data connector: {connector_name}")
        return connector

    # =========================================================================
    # Network and State Management
    # =========================================================================

    async def _start_connector_network(self, connector: ConnectorBase):
        """Start connector network tasks."""
        try:
            await self._stop_connector_network(
                connector,
                getattr(connector, "_api_cleanup_account", "__unscoped__"),
                getattr(connector, "_api_cleanup_connector", "__unscoped__"),
            )

            # Gateway/AMM connectors use start_network() instead of individual polling tasks
            if hasattr(connector, '_trading_rules_polling_loop'):
                connector._trading_rules_polling_task = safe_ensure_future(
                    connector._trading_rules_polling_loop()
                )
            if hasattr(connector, '_trading_fees_polling_loop'):
                connector._trading_fees_polling_task = safe_ensure_future(
                    connector._trading_fees_polling_loop()
                )
            if hasattr(connector, '_create_user_stream_tracker_task'):
                connector._user_stream_tracker_task = connector._create_user_stream_tracker_task()
            if hasattr(connector, '_user_stream_event_listener'):
                connector._user_stream_event_listener_task = safe_ensure_future(
                    connector._user_stream_event_listener()
                )
            if hasattr(connector, '_lost_orders_update_polling_loop'):
                connector._lost_orders_update_task = safe_ensure_future(
                    connector._lost_orders_update_polling_loop()
                )

            # For gateway connectors, call start_network() which handles chain/network detection
            if hasattr(connector, 'start_network') and not hasattr(connector, '_trading_rules_polling_loop'):
                await connector.start_network()

            # NOTE: Order book tracker is started lazily when first trading pair is added
            # (in _add_trading_pair_to_tracker). Starting it here with no subscriptions
            # causes exchanges like Binance to immediately disconnect (close code 1008).

            logger.debug("Started network tasks for connector")

        except Exception as e:
            logger.error(f"Error starting connector network: {e}")
            raise

    @staticmethod
    def _tasks_in_value(value) -> Set[asyncio.Task]:
        """Collect tasks from the task containers used by tracker generations."""
        if isinstance(value, asyncio.Task):
            return {value}
        if isinstance(value, dict):
            values = value.values()
        elif isinstance(value, (list, tuple, set, frozenset)):
            values = value
        else:
            return set()
        found: Set[asyncio.Task] = set()
        for item in values:
            found.update(UnifiedConnectorService._tasks_in_value(item))
        return found

    @staticmethod
    def _collect_tracker_tasks(tracker) -> Set[asyncio.Task]:
        task_names = (
            "_init_order_books_task",
            "_emit_trade_event_task",
            "_order_book_stream_listener_task",
            "_order_book_diff_listener_task",
            "_order_book_trade_listener_task",
            "_order_book_snapshot_listener_task",
            "_order_book_snapshot_router_task",
            "_order_book_diff_router_task",
            "_order_book_trade_event_listener_task",
            "_update_last_trade_prices_task",
        )
        found: Set[asyncio.Task] = set()
        for task_name in task_names:
            found.update(
                UnifiedConnectorService._tasks_in_value(
                    getattr(tracker, task_name, None)
                )
            )
        for attr_name in (
            "_tracking_tasks",
            "_generation_tasks",
            "_draining_tasks",
            "_generation_drain_tasks",
        ):
            found.update(
                UnifiedConnectorService._tasks_in_value(
                    getattr(tracker, attr_name, None)
                )
            )
        for attr_name, value in getattr(tracker, "__dict__", {}).items():
            if "task" in attr_name:
                found.update(UnifiedConnectorService._tasks_in_value(value))
        found.discard(asyncio.current_task())
        return found

    @staticmethod
    def _collect_connector_tasks(connector: ConnectorBase) -> Set[asyncio.Task]:
        """Capture every known real connector/tracker task before stop nulls it."""
        task_names = (
            '_trading_rules_polling_task',
            '_trading_fees_polling_task',
            '_status_polling_task',
            '_user_stream_tracker_task',
            '_user_stream_event_listener_task',
            '_lost_orders_update_task',
        )
        tracker_task_names = (
            '_init_order_books_task',
            '_emit_trade_event_task',
            '_order_book_stream_listener_task',
            '_order_book_diff_listener_task',
            '_order_book_trade_listener_task',
            '_order_book_snapshot_listener_task',
            '_order_book_snapshot_router_task',
            '_order_book_diff_router_task',
            '_order_book_trade_event_listener_task',
            '_update_last_trade_prices_task',
        )
        current = asyncio.current_task()
        found: Set[asyncio.Task] = set()
        for task_name in task_names:
            task = getattr(connector, task_name, None)
            if isinstance(task, asyncio.Task) and task is not current:
                found.add(task)
        tracker = getattr(connector, "order_book_tracker", None)
        if tracker is not None:
            for task_name in tracker_task_names:
                task = getattr(tracker, task_name, None)
                if isinstance(task, asyncio.Task) and task is not current:
                    found.add(task)
            for attr_name in (
                "_tracking_tasks",
                "_generation_tasks",
                "_draining_tasks",
                "_generation_drain_tasks",
            ):
                found.update(
                    UnifiedConnectorService._tasks_in_value(
                        getattr(tracker, attr_name, None)
                    )
                )
            # Accepted tracker variants may add generation-owned task containers.
            for attr_name, value in getattr(tracker, "__dict__", {}).items():
                if "task" in attr_name:
                    found.update(UnifiedConnectorService._tasks_in_value(value))
            found.update(UnifiedConnectorService._collect_tracker_tasks(tracker))
        found.discard(current)
        return found

    async def _stop_connector_network(
        self,
        connector: ConnectorBase,
        account_name: str = "__unscoped__",
        connector_name: str = "__unscoped__",
    ):
        """Stop connector tasks with a hard deadline and retain pending handles."""
        self._ensure_connector_cleanup_state()
        owner_key = self._owner_key(account_name, connector_name, "network")
        self._connector_cleanup_owners[owner_key] = connector
        if not await self._drain_owner_tasks(owner_key):
            raise RuntimeError("task_cleanup_pending")
        task_names = (
            '_trading_rules_polling_task',
            '_trading_fees_polling_task',
            '_status_polling_task',
            '_user_stream_tracker_task',
            '_user_stream_event_listener_task',
            '_lost_orders_update_task',
        )
        tracker_task_names = (
            '_init_order_books_task',
            '_emit_trade_event_task',
            '_order_book_stream_listener_task',
            '_order_book_diff_listener_task',
            '_order_book_trade_listener_task',
            '_order_book_snapshot_listener_task',
            '_order_book_snapshot_router_task',
            '_order_book_diff_router_task',
            '_order_book_trade_event_listener_task',
            '_update_last_trade_prices_task',
        )
        tasks = self._collect_connector_tasks(connector)
        cleanup_codes = []
        for task_name in task_names:
            task = getattr(connector, task_name, None)
            if isinstance(task, asyncio.Task):
                setattr(connector, task_name, None)

        # Stop the order book tracker (if connector has one - AMM/Gateway connectors don't)
        if hasattr(connector, 'order_book_tracker') and connector.order_book_tracker:
            try:
                await self._run_owner_operation(
                    owner_key,
                    "tracker_stop",
                    connector.order_book_tracker.stop,
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:
                code = exc.args[0] if isinstance(exc, RuntimeError) and exc.args else None
                cleanup_codes.append(
                    code
                    if code in {
                        "tracker_stop_pending",
                        "tracker_stop_cancelled",
                        "tracker_stop_raised",
                        "tracker_stop_returned_false",
                    }
                    else "tracker_stop_failed"
                )

        # For gateway connectors, call stop_network()
        if hasattr(connector, 'stop_network'):
            try:
                await self._run_owner_operation(
                    owner_key,
                    "connector_stop",
                    connector.stop_network,
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:
                code = exc.args[0] if isinstance(exc, RuntimeError) and exc.args else None
                cleanup_codes.append(
                    code
                    if code in {
                        "connector_stop_pending",
                        "connector_stop_cancelled",
                        "connector_stop_raised",
                        "connector_stop_returned_false",
                    }
                    else "connector_stop_failed"
                )

        tasks.update(self._collect_connector_tasks(connector))
        if not await self._drain_owner_tasks(owner_key, tasks):
            cleanup_codes.append("task_cleanup_pending")

        tracker = getattr(connector, "order_book_tracker", None)
        for task_name in task_names:
            if getattr(connector, task_name, None) in tasks:
                setattr(connector, task_name, None)
        if tracker is not None:
            for task_name in tracker_task_names:
                if getattr(tracker, task_name, None) in tasks:
                    setattr(tracker, task_name, None)
        for attr_name in (
            "_tracking_tasks",
            "_generation_tasks",
            "_draining_tasks",
            "_generation_drain_tasks",
        ):
            task_mapping = getattr(tracker, attr_name, None)
            if isinstance(task_mapping, dict):
                for key, task in tuple(task_mapping.items()):
                    if task in tasks and task.done():
                        task_mapping.pop(key, None)
        if cleanup_codes:
            raise RuntimeError(cleanup_codes[0])
        if not await self._drain_owner_tasks(owner_key):
            raise RuntimeError("task_cleanup_pending")
        self._finish_owner_cleanup(owner_key)

    async def _stop_data_connector_network(
        self,
        connector: ConnectorBase,
        connector_name: str,
    ) -> None:
        """Stop one published data connector without double-stopping its tracker."""
        self._ensure_connector_cleanup_state()
        owner_key = self._owner_key("__data__", connector_name, "data_network")
        self._connector_cleanup_owners[owner_key] = connector
        if not await self._drain_owner_tasks(owner_key):
            raise RuntimeError("task_cleanup_pending")

        tasks = self._collect_connector_tasks(connector)
        stop_network = getattr(connector, "stop_network", None)
        if stop_network is None:
            raise RuntimeError("connector_stop_unavailable")
        await self._run_owner_operation(
            owner_key,
            "connector_stop",
            stop_network,
        )
        tasks.update(self._collect_connector_tasks(connector))
        if not await self._drain_owner_tasks(owner_key, tasks):
            raise RuntimeError("task_cleanup_pending")
        self._finish_owner_cleanup(owner_key)

    async def refresh_connector_state(
        self,
        connector: ConnectorBase,
        connector_name: str,
        account_name: str = None
    ):
        """Public API to refresh a single connector's state (balances, positions, orders).

        Delegates to the internal _update_connector_state implementation so callers
        in sibling services don't depend on the underscore-prefixed helper.
        """
        await self._update_connector_state(connector, connector_name, account_name)

    async def _update_connector_state(
        self,
        connector: ConnectorBase,
        connector_name: str,
        account_name: str = None
    ):
        """Update connector state (balances, positions, orders).

        Note: Trading rules are NOT refreshed here — the background
        _trading_rules_polling_loop() (started in _start_connector_network)
        already handles that.
        """
        try:
            connector._set_current_timestamp(time.time())
            await connector._update_balances()

            if self._is_perpetual_connector(connector):
                await connector._update_positions()

            if connector.in_flight_orders:
                await connector._update_order_status()
                if account_name:
                    await self._sync_orders_to_database(
                        connector, account_name, connector_name
                    )

        except Exception as e:
            logger.error(f"Error updating connector state: {e}")

    async def _verify_restart_owner_phases(
        self, owner_key: Tuple[str, str, str]
    ) -> bool:
        """Consume retained restart stop/start results before a final stop."""
        self._ensure_connector_cleanup_state()
        owner = self._connector_cleanup_owners.get(owner_key)
        if owner is None:
            return True
        tracker = owner.get("tracker") if isinstance(owner, dict) else owner
        if tracker is None:
            return False
        phases = self._connector_cleanup_phases.get(owner_key, {})
        verified = True
        for phase_name, method_name in (
            ("restart_stop", "stop"),
            ("restart_start", "start"),
        ):
            if phase_name not in phases:
                continue
            operation = getattr(tracker, method_name)
            if phase_name == "restart_start" and isinstance(owner, dict):
                owner["start_may_activate"] = True
            try:
                await self._run_owner_operation(
                    owner_key, phase_name, operation
                )
                if phase_name == "restart_start" and isinstance(owner, dict):
                    owner["tracker_started"] = True
            except asyncio.CancelledError:
                raise
            except BaseException:
                # A terminal false/raise/cancel is still an inspected result:
                # the final-generation stop must run because start may already
                # have activated. Only a nonterminal retained task defers it.
                phase = phases[phase_name]
                task = phase.get("task")
                if not phase.get("complete") and task is not None and task.done():
                    try:
                        await self._run_owner_operation(
                            owner_key, phase_name, operation
                        )
                    except asyncio.CancelledError:
                        raise
                    except BaseException:
                        pass
                if not phase.get("complete"):
                    verified = False
        return verified and await self._drain_owner_tasks(owner_key)

    async def _cleanup_restart_owner(
        self,
        owner_key: Tuple[str, str, str],
        stopped_tracker=None,
    ) -> bool:
        """Finish one restart owner, coalescing a later network final stop."""
        self._ensure_connector_cleanup_state()
        owner = self._connector_cleanup_owners.get(owner_key)
        if owner is None:
            self._finish_owner_cleanup(owner_key)
            return True
        tracker = owner.get("tracker") if isinstance(owner, dict) else owner
        if tracker is None or not await self._verify_restart_owner_phases(owner_key):
            return False

        phases = self._connector_cleanup_phases.get(owner_key, {})
        start_may_activate = isinstance(owner, dict) and owner.get(
            "start_may_activate", False
        )
        if stopped_tracker is tracker:
            self._finish_owner_cleanup(owner_key)
            return True

        # A completed restart_stop is the pre-start generation. It proves final
        # inactivity only when no start task could subsequently activate.
        if "restart_stop" in phases and not start_may_activate:
            self._finish_owner_cleanup(owner_key)
            return True

        tracker_tasks = self._collect_tracker_tasks(tracker)
        try:
            await self._run_owner_operation(
                owner_key, "restart_cleanup_stop", tracker.stop
            )
        except asyncio.CancelledError:
            raise
        except BaseException:
            return False
        tracker_tasks.update(self._collect_tracker_tasks(tracker))
        if not await self._drain_owner_tasks(owner_key, tracker_tasks):
            return False
        self._finish_owner_cleanup(owner_key)
        return True

    async def update_all_trading_connector_states(self):
        """Update state for all trading connectors in parallel."""
        tasks = []
        task_keys = []
        for account_name, connectors in self._trading_connectors.items():
            for connector_name, connector in connectors.items():
                tasks.append(self._update_connector_state(connector, connector_name, account_name))
                task_keys.append(f"{account_name}/{connector_name}")
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for key, result in zip(task_keys, results):
                if isinstance(result, Exception):
                    logger.error(f"Error updating {key}: {result}")

    async def initialize_all_trading_connectors(self):
        """
        Initialize all trading connectors for all accounts at startup.

        This ensures that:
        1. All connectors are ready to use immediately
        2. Existing orders from database are loaded into in_flight_orders
        3. Order tracking and cancellation work without needing manual initialization
        """
        # Get list of all accounts
        accounts = fs_util.list_folders('credentials')

        total_initialized = 0
        for account_name in accounts:
            # Get all connector credentials for this account
            connector_names = self.list_available_credentials(account_name)

            for connector_name in connector_names:
                try:
                    logger.info(f"Initializing connector: {account_name}/{connector_name}")
                    await self.get_trading_connector(account_name, connector_name)
                    total_initialized += 1
                except Exception as e:
                    logger.error(f"Failed to initialize {account_name}/{connector_name}: {e}")
                    # Continue with other connectors even if one fails
                    continue

        logger.info(f"Initialized {total_initialized} trading connectors across {len(accounts)} accounts")

    # =========================================================================
    # Order Management
    # =========================================================================

    async def _load_existing_orders(
        self,
        connector: ConnectorBase,
        account_name: str,
        connector_name: str
    ):
        """Load existing orders from database into connector."""
        try:
            from database import OrderRepository

            async with self.db_manager.get_session_context() as session:
                order_repo = OrderRepository(session)
                # The connector needs the complete in-flight book: any cap here would
                # leave live exchange orders invisible to sync and reconciliation.
                active_orders = await order_repo.get_active_orders(
                    account_name=account_name,
                    connector_name=connector_name,
                    limit=None
                )

                for order_record in active_orders:
                    try:
                        in_flight_order = self._convert_db_order_to_in_flight(order_record)
                        connector.in_flight_orders[in_flight_order.client_order_id] = in_flight_order
                    except Exception as e:
                        logger.error(f"Error loading order {order_record.client_order_id}: {e}")

                logger.info(
                    f"Loaded {len(connector.in_flight_orders)} orders for "
                    f"{account_name}/{connector_name}"
                )

        except Exception as e:
            logger.error(f"Error loading orders from database: {e}")

    async def _sync_orders_to_database(
        self,
        connector: ConnectorBase,
        account_name: str,
        connector_name: str
    ):
        """Sync connector's in_flight_orders state to database."""
        if not self.db_manager:
            return

        from database import OrderRepository

        terminal_states = [
            OrderState.FILLED, OrderState.CANCELED,
            OrderState.FAILED, OrderState.COMPLETED
        ]
        orders_to_remove = []

        try:
            # Single session/transaction per connector: one batched SELECT for the whole book,
            # one flush for the statuses that actually moved, and one commit on context exit.
            async with self.db_manager.get_session_context() as session:
                order_repo = OrderRepository(session)

                in_flight_orders = list(connector.in_flight_orders.items())
                db_orders = await order_repo.get_orders_by_client_ids(
                    [client_order_id for client_order_id, _ in in_flight_orders]
                )
                db_orders_by_client_id = {
                    db_order.client_order_id: db_order for db_order in db_orders
                }

                status_changed = False
                for client_order_id, order in in_flight_orders:
                    try:
                        # Orders held in memory but absent from the DB are simply skipped.
                        db_order = db_orders_by_client_id.get(client_order_id)

                        if db_order:
                            new_status = self._map_order_state_to_status(order.current_state)
                            if db_order.status != new_status:
                                db_order.status = new_status
                                status_changed = True

                        if order.current_state in terminal_states:
                            orders_to_remove.append(client_order_id)

                    except Exception as e:
                        logger.error(f"Error syncing order {client_order_id}: {e}")

                if status_changed:
                    await session.flush()

        except Exception as e:
            logger.error(f"Error syncing orders for {account_name}/{connector_name}: {e}")

        for order_id in orders_to_remove:
            connector.in_flight_orders.pop(order_id, None)

    @staticmethod
    def _supports_order_status_query(connector: ConnectorBase) -> bool:
        """Whether a connector can be asked the real state of a single order."""
        return (
            hasattr(connector, "_request_order_status")
            and hasattr(connector, "_is_order_not_found_during_status_update_error")
            and hasattr(connector, "in_flight_orders")
        )

    async def reconcile_active_orders(self) -> Dict[str, int]:
        """Reconcile persisted active orders against the exchange on startup.

        Must be called AFTER ``initialize_all_trading_connectors`` so that each
        connector's persisted active orders have been reloaded into
        ``in_flight_orders``. For every tracked order we ask the exchange for its
        real state (``_request_order_status``) and:

        - **Confirmed terminal** (filled/cancelled/failed) -> update the DB to the
          real status and drop it from tracking.
        - **Confirmed not found** on the exchange -> mark it CANCELLED in the DB
          (it no longer exists) and drop it from tracking.
        - **Still open** -> sync the DB to the live state and KEEP it tracked, so
          it remains visible and cancelable via the trading endpoints.
        - **Unverifiable** (transient error, or the connector isn't available)
          -> leave the order untouched. We never mark an order terminal unless
          the exchange confirms it, so a connector that failed to start can never
          cause a live order to be falsely reported as cancelled.

        Returns a summary dict with counts for logging/telemetry.
        """
        summary = {"reconciled_terminal": 0, "still_open": 0, "unverified": 0, "skipped_connectors": 0}
        if not self.db_manager:
            return summary

        terminal_states = {
            OrderState.FILLED, OrderState.CANCELED,
            OrderState.FAILED, OrderState.COMPLETED,
        }

        from database import OrderRepository

        for account_name, connectors in self._trading_connectors.items():
            for connector_name, connector in connectors.items():
                if not self._supports_order_status_query(connector) or not connector.in_flight_orders:
                    continue

                # Snapshot tracked orders (the set was loaded from the DB at init).
                tracked_orders = list(connector.in_flight_orders.values())
                # Orders whose real state the exchange confirmed, as (client_order_id, state).
                # Tracking is only updated once the transaction has committed.
                resolved_orders = []
                unverified = 0
                try:
                    # Single session/transaction per connector: one batched SELECT for the whole
                    # tracked book, one flush for the statuses that actually moved, and one
                    # commit on context exit.
                    async with self.db_manager.get_session_context() as session:
                        order_repo = OrderRepository(session)
                        db_orders = await order_repo.get_orders_by_client_ids(
                            [order.client_order_id for order in tracked_orders]
                        )
                        db_orders_by_client_id = {
                            db_order.client_order_id: db_order for db_order in db_orders
                        }

                        status_changed = False
                        for order in tracked_orders:
                            client_order_id = order.client_order_id
                            note = None
                            try:
                                order_update = await connector._request_order_status(order)
                                new_state = order_update.new_state
                            except Exception as exc:
                                if connector._is_order_not_found_during_status_update_error(exc):
                                    # The exchange does not know this order -> it is gone.
                                    new_state = OrderState.CANCELED
                                    note = "Reconciled on startup: order not found on exchange"
                                else:
                                    # Transient/unknown error - do not touch the order.
                                    logger.warning(
                                        f"Could not verify order {client_order_id} on "
                                        f"{account_name}/{connector_name}: {exc}"
                                    )
                                    unverified += 1
                                    continue

                            db_status = self._map_order_state_to_status(new_state)
                            # Orders tracked in memory but absent from the DB have no row to
                            # correct; they are still reconciled against the exchange.
                            db_order = db_orders_by_client_id.get(client_order_id)
                            if db_order is not None:
                                if db_order.status != db_status:
                                    db_order.status = db_status
                                    status_changed = True
                                if note and db_order.error_message != note:
                                    db_order.error_message = note
                                    status_changed = True

                            resolved_orders.append((client_order_id, new_state))

                        if status_changed:
                            await session.flush()
                except Exception as exc:
                    # The whole batch failed to persist: nothing is untracked and every order
                    # stays as it was, to be reconciled again on the next startup.
                    logger.error(
                        f"Failed to persist reconciled orders for "
                        f"{account_name}/{connector_name}: {exc}"
                    )
                    summary["unverified"] += len(tracked_orders)
                    continue

                summary["unverified"] += unverified
                for client_order_id, new_state in resolved_orders:
                    if new_state in terminal_states:
                        connector.in_flight_orders.pop(client_order_id, None)
                        summary["reconciled_terminal"] += 1
                    else:
                        # Keep tracking so it stays cancelable via the trading endpoints.
                        summary["still_open"] += 1

        logger.info(
            "Order reconciliation complete: "
            f"{summary['reconciled_terminal']} closed, "
            f"{summary['still_open']} still open (re-tracked), "
            f"{summary['unverified']} unverified"
        )
        return summary

    async def sync_all_orders_to_database(self):
        """
        Sync connector's in_flight_orders state to database for all trading connectors.

        The connector's built-in polling already updates in_flight_orders from the exchange.
        This method syncs that state to our database and cleans up closed orders.
        """
        tasks = []
        task_keys = []
        for account_name, connectors in self._trading_connectors.items():
            for connector_name, connector in connectors.items():
                if not connector.in_flight_orders:
                    continue
                tasks.append(self._sync_orders_to_database(connector, account_name, connector_name))
                task_keys.append(f"{account_name}/{connector_name}")
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for key, result in zip(task_keys, results):
                if isinstance(result, Exception):
                    logger.error(f"Error syncing order state for {key}: {result}")
                else:
                    logger.debug(f"Synced order state to DB for {key}")

    def _convert_db_order_to_in_flight(self, order_record) -> InFlightOrder:
        """Convert database order to InFlightOrder."""
        status_mapping = {
            "SUBMITTED": OrderState.PENDING_CREATE,
            "OPEN": OrderState.OPEN,
            "PARTIALLY_FILLED": OrderState.PARTIALLY_FILLED,
            "FILLED": OrderState.FILLED,
            "CANCELLED": OrderState.CANCELED,
            "FAILED": OrderState.FAILED,
        }

        order_state = status_mapping.get(order_record.status, OrderState.PENDING_CREATE)

        try:
            order_type = OrderType[order_record.order_type]
        except (KeyError, ValueError):
            order_type = OrderType.LIMIT

        try:
            trade_type = TradeType[order_record.trade_type]
        except (KeyError, ValueError):
            trade_type = TradeType.BUY

        creation_timestamp = (
            order_record.created_at.timestamp()
            if order_record.created_at else time.time()
        )

        in_flight_order = InFlightOrder(
            client_order_id=order_record.client_order_id,
            trading_pair=order_record.trading_pair,
            order_type=order_type,
            trade_type=trade_type,
            amount=Decimal(str(order_record.amount)),
            creation_timestamp=creation_timestamp,
            price=Decimal(str(order_record.price)) if order_record.price else None,
            exchange_order_id=order_record.exchange_order_id,
            initial_state=order_state,
            leverage=1,
            position=PositionAction.NIL,
        )

        in_flight_order.current_state = order_state
        if order_record.filled_amount:
            in_flight_order.executed_amount_base = Decimal(str(order_record.filled_amount))

        return in_flight_order

    def _map_order_state_to_status(self, order_state: OrderState) -> str:
        """Map OrderState to database status string."""
        mapping = {
            OrderState.PENDING_CREATE: "SUBMITTED",
            OrderState.OPEN: "OPEN",
            OrderState.PENDING_CANCEL: "PENDING_CANCEL",
            OrderState.CANCELED: "CANCELLED",
            OrderState.PARTIALLY_FILLED: "PARTIALLY_FILLED",
            OrderState.FILLED: "FILLED",
            OrderState.FAILED: "FAILED",
            OrderState.PENDING_APPROVAL: "SUBMITTED",
            OrderState.APPROVED: "SUBMITTED",
            OrderState.CREATED: "SUBMITTED",
            OrderState.COMPLETED: "FILLED",
        }
        return mapping.get(order_state, "SUBMITTED")

    # =========================================================================
    # Metrics
    # =========================================================================

    def _initialize_metrics(
        self,
        connector: ConnectorBase,
        account_name: str,
        connector_name: str,
        cache_key: str
    ):
        """Initialize trade volume metrics collector."""
        if cache_key in self._metrics_collectors:
            return

        if "_paper_trade" in connector_name:
            return

        try:
            instance_id = f"{account_name}_hbotapi"
            rate_provider = self._rate_provider
            if rate_provider is None:
                logger.debug(f"No rate provider set, skipping trade-volume metrics for {connector_name}")
                return

            metrics_collector = TradeVolumeMetricCollector(
                connector=connector,
                activation_interval=self.METRICS_ACTIVATION_INTERVAL,
                rate_provider=rate_provider,
                instance_id=instance_id,
                valuation_token=self.METRICS_VALUATION_TOKEN
            )
            metrics_collector.start()
            self._metrics_collectors[cache_key] = metrics_collector

        except Exception as e:
            logger.warning(f"Failed to init metrics for {connector_name}: {e}")

    # =========================================================================
    # Credentials and Configuration
    # =========================================================================

    async def update_connector_keys(
        self,
        account_name: str,
        connector_name: str,
        keys: dict
    ) -> ConnectorBase:
        """Update API keys and recreate connector."""
        if not BackendAPISecurity.login_account(
            account_name=account_name,
            secrets_manager=self.secrets_manager
        ):
            raise ValueError(f"Failed to authenticate for {account_name}")

        connector_config = HummingbotAPIConfigAdapter(
            AllConnectorSettings.get_connector_config_keys(connector_name)
        )

        for key, value in keys.items():
            setattr(connector_config, key, value)

        BackendAPISecurity.update_connector_keys(account_name, connector_config)
        BackendAPISecurity.decrypt_all(account_name=account_name)

        # Properly stop old connector (stops recorders, network tasks, cleans up caches)
        await self.stop_trading_connector(account_name, connector_name)

        # Create new connector with fresh recorders
        return await self.get_trading_connector(account_name, connector_name)

    def clear_trading_connector(
        self,
        account_name: Optional[str] = None,
        connector_name: Optional[str] = None
    ):
        """Clear trading connector from cache."""
        if account_name and connector_name:
            if account_name in self._trading_connectors:
                self._trading_connectors[account_name].pop(connector_name, None)
        elif account_name:
            self._trading_connectors.pop(account_name, None)
        else:
            self._trading_connectors.clear()

    def list_account_connectors(self, account_name: str) -> List[str]:
        """List initialized connectors for an account."""
        return list(self._trading_connectors.get(account_name, {}).keys())

    def list_available_credentials(self, account_name: str) -> List[str]:
        """List connector credentials available for an account."""
        try:
            files = fs_util.list_files(f"credentials/{account_name}/connectors")
            return [f.replace(".yml", "") for f in files if f.endswith(".yml")]
        except FileNotFoundError:
            return []

    @staticmethod
    def get_connector_config_map(connector_name: str):
        """Get connector config field info."""
        from typing import Literal, get_args, get_origin

        connector_config = HummingbotAPIConfigAdapter(
            AllConnectorSettings.get_connector_config_keys(connector_name)
        )
        fields_info = {}

        for key, field in connector_config.hb_config.model_fields.items():
            if key == "connector":
                continue

            field_type = field.annotation
            type_name = getattr(field_type, "__name__", str(field_type))
            allowed_values = None

            origin = get_origin(field_type)
            args = get_args(field_type)

            if origin is Literal:
                type_name = "Literal"
                allowed_values = list(args)
            elif origin is not None:
                if type(None) in args:
                    actual_types = [arg for arg in args if arg is not type(None)]
                    if actual_types:
                        inner_type = actual_types[0]
                        inner_origin = get_origin(inner_type)
                        inner_args = get_args(inner_type)
                        if inner_origin is Literal:
                            type_name = "Literal"
                            allowed_values = list(inner_args)
                        else:
                            type_name = getattr(inner_type, "__name__", str(inner_type))
                else:
                    type_name = str(field_type)

            field_info = {"type": type_name, "required": field.is_required()}
            if allowed_values is not None:
                field_info["allowed_values"] = allowed_values

            prompt = UnifiedConnectorService._resolve_field_prompt(
                field, connector_config.hb_config
            )
            if prompt is not None:
                field_info["prompt"] = prompt

            fields_info[key] = field_info

        return fields_info

    @staticmethod
    def _resolve_field_prompt(field, config_map) -> Optional[str]:
        """Resolve the Hummingbot prompt of a config field, if it defines one.

        Prompts are stored in ``json_schema_extra`` either as a plain string or as
        a callable taking the config map, so callables are evaluated here.
        """
        extra = field.json_schema_extra
        if not isinstance(extra, dict):
            return None

        prompt = extra.get("prompt")
        if prompt is None:
            return None

        if callable(prompt):
            try:
                prompt = prompt(config_map)
            except Exception as e:
                logger.debug(f"Could not resolve prompt for config field: {e}")
                return None

        return str(prompt) if prompt is not None else None

    # =========================================================================
    # Cleanup
    # =========================================================================

    async def stop_trading_connector(self, account_name: str, connector_name: str):
        """Stop a trading connector, exposing any residual instead of hiding it."""
        cleanup_task = asyncio.create_task(
            self._locked_stop_trading_connector(account_name, connector_name)
        )
        cancelled = False
        while not cleanup_task.done():
            try:
                await asyncio.shield(cleanup_task)
            except asyncio.CancelledError:
                cancelled = True
        detail = cleanup_task.result()
        if cancelled:
            raise asyncio.CancelledError()
        if detail is not None:
            raise ConnectorCapabilityError(detail)
        return True

    async def _locked_stop_trading_connector(
        self, account_name: str, connector_name: str
    ) -> Optional[CapabilityErrorDetail]:
        """Use the same per-key transaction lock for ordinary stop and publish."""
        connector_key = self._connector_key(account_name, connector_name)
        async with self._connector_creation_lock(connector_key):
            return await self._finalize_stop_trading_connector(
                account_name, connector_name
            )

    async def _finalize_stop_trading_connector(
        self, account_name: str, connector_name: str
    ) -> Optional[CapabilityErrorDetail]:
        """Run bounded connector cleanup and retain failed indices for an explicit retry."""
        self._ensure_connector_cleanup_state()
        connector_key = self._connector_key(account_name, connector_name)
        cache_key = self._index_key(account_name, connector_name)
        cleanup_codes = []
        retained_indices = self._connector_cleanup_indices
        retained_for_key = retained_indices.get(connector_key, {})
        indices = (
            ("orders", self._orders_recorders),
            ("funding", self._funding_recorders),
            ("metrics", self._metrics_collectors),
        )
        for label, mapping in indices:
            value = mapping.get(cache_key)
            if value is None:
                value = retained_for_key.get(label)
            if value is None:
                value = self._connector_cleanup_owners.get(
                    self._owner_key(account_name, connector_name, label)
                )
            if value is None:
                continue
            try:
                await self._stop_owned_index(
                    account_name, connector_name, label, value
                )
            except BaseException:
                cleanup_codes.append(f"{label}_cleanup_incomplete")
                retained_for_key[label] = value
            else:
                retained_for_key.pop(label, None)
                if mapping.get(cache_key) is value:
                    mapping.pop(cache_key, None)

        restart_owner_keys = {
            owner_key
            for registry in (
                self._connector_cleanup_owners,
                self._connector_residual_tasks,
                self._connector_cleanup_phases,
            )
            for owner_key in registry
            if owner_key[:2] == connector_key and owner_key[2] == "restart"
        }
        verified_restart_owners = set()
        for owner_key in sorted(restart_owner_keys):
            if await self._verify_restart_owner_phases(owner_key):
                verified_restart_owners.add(owner_key)
            else:
                cleanup_codes.append("restart_cleanup_incomplete")

        restart_phase_failed = bool(
            restart_owner_keys - verified_restart_owners
        )
        account_connectors = self._trading_connectors.get(account_name)
        data_connector = (
            self._data_connectors.get(connector_name)
            if account_name == "__data__"
            else None
        )
        connector = data_connector
        if connector is None and account_connectors:
            connector = account_connectors.get(connector_name)
        stopped_connectors = []
        network_cleanup_failed = restart_phase_failed
        if connector is None:
            connector = self._connector_cleanup_owners.get(
                self._owner_key(account_name, connector_name, "network")
            )
        if connector is not None and not restart_phase_failed:
            try:
                if connector is data_connector:
                    await self._stop_data_connector_network(
                        connector, connector_name
                    )
                else:
                    await self._stop_connector_network(
                        connector, account_name, connector_name
                    )
            except BaseException:
                cleanup_codes.append("connector_cleanup_incomplete")
                network_cleanup_failed = True
            else:
                stopped_connectors.append(connector)
                if connector is data_connector:
                    if self._data_connectors.get(connector_name) is connector:
                        self._data_connectors.pop(connector_name, None)
                    self._data_connectors_started.pop(connector_name, None)
                elif account_connectors and account_connectors.get(connector_name) is connector:
                    account_connectors.pop(connector_name, None)
                if account_connectors is not None and not account_connectors:
                    self._trading_connectors.pop(account_name, None)

        # Failed market transactions can be the sole authority retaining an
        # unpublished connector. Explicit connector stop consumes those private
        # owner handles as part of the same account/connector transaction.
        market_owners = getattr(self, "_market_cleanup_owners", {})
        market_residuals = getattr(self, "_market_cleanup_residuals", {})
        market_phases = getattr(self, "_market_cleanup_phases", {})
        for market_key in list(market_owners):
            if market_key[:2] != connector_key:
                continue
            owner = market_owners.get(market_key, {})
            market_connector = (
                owner.get("connector") if isinstance(owner, dict) else None
            )
            if (
                market_connector is not None
                and not any(
                    market_connector is stopped for stopped in stopped_connectors
                )
            ):
                if restart_phase_failed:
                    continue
                try:
                    await self._stop_connector_network(
                        market_connector, account_name, connector_name
                    )
                except BaseException:
                    cleanup_codes.append("market_owner_cleanup_incomplete")
                    network_cleanup_failed = True
                    continue
                stopped_connectors.append(market_connector)
            market_owners.pop(market_key, None)
            market_residuals.pop(market_key, None)

        # Network shutdown is the final-generation stop for the same published
        # tracker. Private-only or distinct restart trackers still need their own
        # final stop after retained phases have been verified.
        for owner_key in sorted(verified_restart_owners):
            owner = self._connector_cleanup_owners.get(owner_key)
            restart_tracker = (
                owner.get("tracker") if isinstance(owner, dict) else owner
            )
            stopped_tracker = next(
                (
                    restart_tracker
                    for stopped_connector in stopped_connectors
                    if getattr(stopped_connector, "order_book_tracker", None)
                    is restart_tracker
                ),
                None,
            )
            if not await self._cleanup_restart_owner(
                owner_key, stopped_tracker=stopped_tracker
            ):
                cleanup_codes.append("restart_cleanup_incomplete")

        # Future owner kinds retain task handles under the same identity.
        for owner_key in list(self._connector_residual_tasks):
            if owner_key[:2] != connector_key:
                continue
            if owner_key[2] == "restart":
                continue
            if not await self._drain_owner_tasks(owner_key):
                cleanup_codes.append(f"{owner_key[2]}_cleanup_incomplete")
            elif owner_key[2] not in {
                "orders",
                "funding",
                "metrics",
                "network",
                "data_network",
            }:
                self._finish_owner_cleanup(owner_key)

        market_tasks = getattr(self, "_market_residual_tasks", {})
        for owner_key in list(market_tasks):
            if owner_key[:2] != connector_key:
                continue
            tasks = market_tasks.pop(owner_key, set())
            pending = {task for task in tasks if not task.done()}
            if pending:
                _, pending = await asyncio.wait(
                    pending, timeout=_TASK_DRAIN_TIMEOUT
                )
            if pending:
                market_tasks[owner_key] = set(pending)
                cleanup_codes.append("market_task_cleanup_incomplete")

        if not network_cleanup_failed and not any(
            key[:2] == connector_key for key in market_tasks
        ):
            for phase_key in list(market_phases):
                if phase_key[:2] == connector_key:
                    market_phases.pop(phase_key, None)

        if not network_cleanup_failed:
            market_pair_owners = getattr(self, "_market_pair_owners", {})
            for pair_key in list(market_pair_owners):
                if pair_key[:2] == connector_key:
                    market_pair_owners.pop(pair_key, None)

        residuals = self._connector_creation_residuals
        remaining_private = any(
            key[:2] == connector_key
            for registry in (
                self._connector_cleanup_owners,
                self._connector_residual_tasks,
                self._connector_cleanup_phases,
                market_owners,
                market_tasks,
                market_phases,
            )
            for key in registry
        )
        if remaining_private and not cleanup_codes:
            cleanup_codes.append("owner_cleanup_incomplete")
        if not cleanup_codes:
            residuals.pop(connector_key, None)
            retained_indices.pop(connector_key, None)
            for key in list(market_residuals):
                if key[:2] == connector_key:
                    market_residuals.pop(key, None)
                    market_owners.pop(key, None)
            logger.info(f"Stopped trading connector {account_name}/{connector_name}")
            return None
        if retained_for_key:
            retained_indices[connector_key] = retained_for_key
        detail = CapabilityErrorDetail(
            code="connector_cleanup_residual",
            message=f"Connector cleanup is incomplete for '{cache_key}'",
            connector=connector_name,
            feature="connector_cleanup",
            reason="cleanup_incomplete",
            cleanup_scope="connector",
            cleanup_code=cleanup_codes[0],
        )
        residuals[connector_key] = detail.model_dump(exclude_none=True)
        return detail

    async def stop_all(self):
        """Stop all connectors and services."""
        self._ensure_connector_cleanup_state()
        connector_keys = {
            (account_name, connector_name)
            for account_name, connectors in self._trading_connectors.items()
            for connector_name in connectors
        }
        connector_keys.update(
            ("__data__", connector_name)
            for connector_name in self._data_connectors
        )

        def add_key(raw_key) -> None:
            if isinstance(raw_key, tuple) and len(raw_key) >= 2:
                connector_keys.add((raw_key[0], raw_key[1]))
            elif isinstance(raw_key, str) and ":" in raw_key:
                connector_keys.add(tuple(raw_key.split(":", 1)))

        for registry in (
            self._connector_creation_residuals,
            self._connector_cleanup_owners,
            self._connector_cleanup_indices,
            self._connector_residual_tasks,
            self._connector_cleanup_phases,
            getattr(self, "_market_cleanup_residuals", {}),
            getattr(self, "_market_cleanup_owners", {}),
            getattr(self, "_market_pair_owners", {}),
            getattr(self, "_market_residual_tasks", {}),
            getattr(self, "_market_cleanup_phases", {}),
            self._orders_recorders,
            self._funding_recorders,
            self._metrics_collectors,
        ):
            for raw_key in registry:
                add_key(raw_key)

        failures = []
        for account_name, connector_name in sorted(connector_keys):
            try:
                await self.stop_trading_connector(account_name, connector_name)
            except BaseException as exc:
                failures.append(exc)
                logger.error(
                    "Connector shutdown incomplete for %s/%s (%s)",
                    account_name,
                    connector_name,
                    type(exc).__name__,
                )

        if failures:
            raise failures[0]
        self._data_connectors.clear()
        self._data_connectors_started.clear()
        logger.info("Stopped all connectors")

    # =========================================================================
    # Order Book Tracker Diagnostics & Restart
    # =========================================================================

    def get_order_book_tracker_diagnostics(
        self,
        connector_name: str,
        account_name: Optional[str] = None
    ) -> Dict:
        """Get diagnostics for a connector's order book tracker.

        Returns information about:
        - Whether the tracker is running
        - Task status (alive/crashed)
        - Metrics (diffs processed, last update, etc.)
        - WebSocket status

        Args:
            connector_name: The connector to diagnose
            account_name: Optional account for trading connector

        Returns:
            Dictionary with diagnostic information
        """
        self.require_capability(connector_name, ConnectorFeature.LIVE_ORDER_BOOK)
        connector = self.get_best_connector_for_market(connector_name, account_name)

        if not connector:
            return {
                "error": {
                    "code": "connector_not_found",
                    "message": "The requested connector is not available",
                }
            }

        diagnostics = {
            "connector_type": type(connector).__name__,
            "connector_name": connector_name,
            "has_order_book_tracker": False,
            "tracker_ready": False,
            "tasks": {},
            "trading_pairs": [],
            "order_books": {},
            "metrics": None,
            "websocket_status": "unknown",
        }

        if not hasattr(connector, 'order_book_tracker') or not connector.order_book_tracker:
            return diagnostics

        tracker = connector.order_book_tracker
        diagnostics["has_order_book_tracker"] = True
        diagnostics["tracker_ready"] = tracker.ready if hasattr(tracker, 'ready') else False

        # Get trading pairs
        if hasattr(tracker, '_trading_pairs'):
            diagnostics["trading_pairs"] = list(tracker._trading_pairs) if isinstance(tracker._trading_pairs, (list, set)) else []

        # Check task status
        task_names = [
            '_order_book_stream_listener_task',
            '_order_book_diff_listener_task',
            '_order_book_trade_listener_task',
            '_order_book_snapshot_listener_task',
            '_order_book_diff_router_task',
            '_order_book_snapshot_router_task',
            '_init_order_books_task',
            '_emit_trade_event_task',
        ]

        for task_name in task_names:
            task = getattr(tracker, task_name, None)
            if task is not None:
                task_status = {
                    "exists": True,
                    "done": task.done(),
                    "cancelled": task.cancelled(),
                }
                if task.done() and not task.cancelled():
                    try:
                        task_error = task.exception()
                    except (asyncio.CancelledError, Exception) as exc:
                        task_error = exc
                    if task_error is not None:
                        logger.error(
                            "Order book tracker task %s failed for %s (%s)",
                            task_name,
                            connector_name,
                            type(task_error).__name__,
                        )
                        task_status["error"] = {
                            "code": "task_failed",
                            "message": "Order book tracker task failed",
                        }
                diagnostics["tasks"][task_name] = task_status
            else:
                diagnostics["tasks"][task_name] = {"exists": False}

        # Check order books
        if hasattr(tracker, 'order_books'):
            for trading_pair, order_book in tracker.order_books.items():
                try:
                    bids, asks = order_book.snapshot
                    best_bid = float(bids.iloc[0]['price']) if len(bids) > 0 else None
                    best_ask = float(asks.iloc[0]['price']) if len(asks) > 0 else None
                    diagnostics["order_books"][trading_pair] = {
                        "best_bid": best_bid,
                        "best_ask": best_ask,
                        "bid_count": len(bids),
                        "ask_count": len(asks),
                        "snapshot_uid": order_book.snapshot_uid if hasattr(order_book, 'snapshot_uid') else None,
                        "last_diff_uid": order_book.last_diff_uid if hasattr(order_book, 'last_diff_uid') else None,
                    }
                except Exception as exc:
                    logger.error(
                        "Order book diagnostics failed for %s/%s (%s)",
                        connector_name,
                        trading_pair,
                        type(exc).__name__,
                    )
                    diagnostics["order_books"][trading_pair] = {
                        "error": {
                            "code": "order_book_diagnostics_failed",
                            "message": "Order book diagnostics are unavailable",
                        }
                    }

        # Get metrics if available
        if hasattr(tracker, 'metrics'):
            try:
                diagnostics["metrics"] = tracker.metrics.to_dict()
            except Exception as exc:
                logger.error(
                    "Order book metrics diagnostics failed for %s (%s)",
                    connector_name,
                    type(exc).__name__,
                )
                diagnostics["metrics"] = {
                    "error": {
                        "code": "order_book_metrics_unavailable",
                        "message": "Order book metrics are unavailable",
                    }
                }

        # Check WebSocket status
        if hasattr(connector, '_orderbook_ds') and connector._orderbook_ds:
            data_source = connector._orderbook_ds
            if hasattr(data_source, '_ws_assistant') and data_source._ws_assistant is not None:
                diagnostics["websocket_status"] = "connected"
            else:
                diagnostics["websocket_status"] = "not_connected"

        return diagnostics

    async def restart_order_book_tracker(
        self,
        connector_name: str,
        account_name: Optional[str] = None
    ) -> Dict:
        """Restart one tracker as a durable owner-scoped transaction."""
        self.require_capability(connector_name, ConnectorFeature.LIVE_ORDER_BOOK)
        self._ensure_connector_cleanup_state()
        while True:
            owner_account, connector, error = self._resolve_restart_connector_owner(
                connector_name, account_name
            )
            if error is not None:
                return {"success": False, "error": error}
            owner_key = self._owner_key(
                owner_account, connector_name, "restart"
            )
            async with self._connector_creation_lock(
                (owner_account, connector_name)
            ):
                if account_name is None:
                    current_owner, current_connector, current_error = (
                        self._resolve_restart_connector_owner(
                            connector_name, account_name
                        )
                    )
                    if current_error is not None:
                        return {"success": False, "error": current_error}
                    if (
                        current_owner != owner_account
                        or current_connector is not connector
                    ):
                        continue
                return await self._restart_order_book_tracker_locked(
                    connector_name, connector, owner_key
                )

    async def _restart_order_book_tracker_locked(
        self,
        connector_name: str,
        connector: Optional[ConnectorBase],
        owner_key: Tuple[str, str, str],
    ) -> Dict:
        owner = self._connector_cleanup_owners.get(owner_key)
        if isinstance(owner, dict):
            connector = owner.get("connector") or connector
            tracker = owner.get("tracker")
        elif owner is not None:
            tracker = owner
            owner = {
                "account_name": owner_key[0],
                "connector_name": connector_name,
                "connector": connector,
                "tracker": tracker,
                "trading_pairs": list(getattr(tracker, "_trading_pairs", ())),
                "tracker_started": False,
                "start_may_activate": False,
            }
            self._connector_cleanup_owners[owner_key] = owner
        else:
            tracker = getattr(connector, "order_book_tracker", None)

        if not await self._drain_owner_tasks(owner_key):
            return {"success": False, "error": "tracker_cleanup_pending"}

        if not connector:
            if owner is None:
                self._finish_owner_cleanup(owner_key)
            return {"success": False, "error": "connector_not_found"}

        if tracker is None:
            if owner is None:
                self._finish_owner_cleanup(owner_key)
            return {"success": False, "error": "order_book_tracker_unavailable"}

        trading_pairs = (
            list(owner.get("trading_pairs", ()))
            if isinstance(owner, dict)
            else list(getattr(tracker, "_trading_pairs", ()))
        )

        if not trading_pairs:
            if owner is None:
                self._finish_owner_cleanup(owner_key)
            return {"success": False, "error": "no_trading_pairs_to_restart"}

        if owner is None:
            owner = {
                "account_name": owner_key[0],
                "connector_name": connector_name,
                "connector": connector,
                "tracker": tracker,
                "trading_pairs": trading_pairs,
                "tracker_started": False,
                "start_may_activate": False,
                "old_tasks": self._collect_tracker_tasks(tracker),
            }
            self._connector_cleanup_owners[owner_key] = owner
        else:
            owner.setdefault("connector", connector)
            owner.setdefault("tracker", tracker)
            owner.setdefault("trading_pairs", trading_pairs)
            owner.setdefault("tracker_started", False)
            owner.setdefault("start_may_activate", False)
            owner.setdefault("old_tasks", self._collect_tracker_tasks(tracker))

        try:
            if not owner.get("old_tasks_drained", False):
                logger.info("Stopping order book tracker for %s", connector_name)
                await self._run_owner_operation(
                    owner_key, "restart_stop", tracker.stop
                )
                owner["old_tasks"].update(self._collect_tracker_tasks(tracker))
                if not await self._drain_owner_tasks(
                    owner_key, set(owner["old_tasks"])
                ):
                    return {"success": False, "error": "tracker_cleanup_pending"}
                owner["old_tasks_drained"] = True
        except asyncio.CancelledError:
            raise
        except RuntimeError as exc:
            code = exc.args[0] if exc.args else None
            if code is not None and code.endswith("_pending"):
                return {"success": False, "error": "tracker_cleanup_pending"}
            logger.error(
                "Order book tracker stop failed for %s (%s)",
                connector_name,
                type(exc).__name__,
            )
            return {"success": False, "error": "tracker_stop_failed"}

        if not owner.get("tracker_started", False):
            try:
                tracker._trading_pairs.clear()
                for trading_pair in trading_pairs:
                    tracker._trading_pairs.append(trading_pair)
                # An awaitable start can finish after its caller is cancelled.
                # Publish that authority before _run_owner_operation's first await.
                owner["start_may_activate"] = True
                await self._run_owner_operation(
                    owner_key, "restart_start", tracker.start
                )
                owner["tracker_started"] = True
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error(
                    "Order book tracker start failed for %s (%s)",
                    connector_name,
                    type(exc).__name__,
                )
                return {"success": False, "error": "tracker_start_failed"}

        try:
            local_ready = await asyncio.wait_for(tracker.wait_ready(), timeout=30.0)
        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            return {"success": False, "error": "tracker_readiness_timeout"}
        except Exception as exc:
            logger.error(
                "Order book tracker readiness failed for %s (%s)",
                connector_name,
                type(exc).__name__,
            )
            return {"success": False, "error": "tracker_readiness_failed"}
        if local_ready is False:
            return {"success": False, "error": "tracker_not_ready"}

        try:
            websocket_ready = await self._wait_for_websocket_ready(
                connector, timeout=10.0
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(
                "Order book tracker websocket readiness failed for %s (%s)",
                connector_name,
                type(exc).__name__,
            )
            return {"success": False, "error": "tracker_websocket_failed"}
        if websocket_ready is not True:
            return {"success": False, "error": "tracker_websocket_not_ready"}

        self._finish_owner_cleanup(owner_key)

        return {
            "success": True,
            "message": f"Order book tracker restarted for {connector_name}",
            "trading_pairs": trading_pairs,
        }
