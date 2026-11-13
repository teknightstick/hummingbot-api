"""
Trading Service - Centralized trading operations with executor-compatible interface.

This service provides trading operations (buy, sell, cancel) using the
UnifiedConnectorService for connector management.
"""
import asyncio
import logging
import time
from contextlib import asynccontextmanager
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Tuple

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import OrderType, PositionAction
from models.connectors import CapabilityErrorDetail
from services.connector_capabilities import (
    ConnectorCapabilityError,
    ConnectorFeature,
)

if TYPE_CHECKING:
    from services.market_data_service import MarketDataService
    from services.unified_connector_service import UnifiedConnectorService


logger = logging.getLogger(__name__)


class AccountTradingInterface:
    """
    ScriptStrategyBase-compatible interface for executor trading.

    This class provides the exact interface that Hummingbot executors expect
    from a strategy object, backed by UnifiedConnectorService.

    Executors use the following interface from strategy:
    - current_timestamp: float property
    - buy(connector_name, trading_pair, amount, order_type, price, position_action) -> str
    - sell(connector_name, trading_pair, amount, order_type, price, position_action) -> str
    - cancel(connector_name, trading_pair, order_id) -> str
    - get_active_orders(connector_name) -> List

    ExecutorBase also accesses:
    - connectors: Dict[str, ConnectorBase] (accessed directly in ExecutorBase.__init__)
    """

    def __init__(
        self,
        connector_service: "UnifiedConnectorService",
        market_data_service: "MarketDataService",
        account_name: str
    ):
        """
        Initialize AccountTradingInterface.

        Args:
            connector_service: UnifiedConnectorService for connector access
            market_data_service: MarketDataService for order book operations
            account_name: Account to use for connectors
        """
        self._connector_service = connector_service
        self._market_data_service = market_data_service
        self._account_name = account_name

        # Track active markets (connector_name -> set of trading_pairs)
        self._markets: Dict[str, Set[str]] = {}
        # Initialization touches connector-wide tracker structures, so transactions for
        # different pairs on the same account/connector must share one bounded lock.
        self._market_locks: Dict[str, asyncio.Lock] = {}
        self._market_lock_users: Dict[str, int] = {}
        self._market_locks_guard = asyncio.Lock()
        self._cleanup_residuals: Dict[Tuple[str, str], Dict[str, Any]] = {}

        # Timestamp tracking
        self._current_timestamp: float = time.time()

        logger.info(f"AccountTradingInterface created for account: {account_name}")

    @property
    def account_name(self) -> str:
        """Return the account name for this trading interface."""
        return self._account_name

    @property
    def connectors(self) -> Dict[str, ConnectorBase]:
        """
        Return connectors for this account from the UnifiedConnectorService.

        This returns the actual connectors that are already initialized and running.
        """
        return self._connector_service.get_account_connectors(self._account_name)

    @property
    def markets(self) -> Dict[str, Set[str]]:
        """Return active markets configuration."""
        return self._markets

    @property
    def cleanup_residuals(self) -> Dict[Tuple[str, str], Dict[str, Any]]:
        """Return observable cleanup failures without exposing mutable ledger state."""
        return {key: dict(value) for key, value in self._cleanup_residuals.items()}

    @property
    def current_timestamp(self) -> float:
        """Return current timestamp (updated by control loop)."""
        return self._current_timestamp

    def update_timestamp(self):
        """Update the current timestamp. Called by ExecutorService control loop."""
        self._current_timestamp = time.time()

    async def ensure_connector(self, connector_name: str) -> ConnectorBase:
        """
        Ensure connector is loaded and available.

        Args:
            connector_name: Name of the connector

        Returns:
            The connector instance
        """
        require = getattr(self._market_data_service, "require_capability", None)
        if require is not None:
            require(connector_name, ConnectorFeature.ORDER_SUBMISSION)
        return await self._connector_service.get_trading_connector(
            self._account_name,
            connector_name
        )

    async def add_market(
        self,
        connector_name: str,
        trading_pair: str,
        order_book_timeout: float = 30.0
    ):
        """
        Add a trading pair to active markets with full order book support.

        This method ensures:
        1. Connector is loaded
        2. Order book is initialized and has valid data
        3. Rate sources are initialized for price feeds

        Args:
            connector_name: Name of the connector
            trading_pair: Trading pair to add
            order_book_timeout: Timeout in seconds to wait for order book data
        """
        require = getattr(self._market_data_service, "require_capability", None)
        if require is not None:
            # Both checks are local declarations. They must run before ensure_connector(),
            # which can authenticate, create a connector, and start network tasks.
            require(connector_name, ConnectorFeature.LIVE_ORDER_BOOK, trading_pair)
            require(connector_name, ConnectorFeature.ORDER_SUBMISSION, trading_pair)

        async with self._market_transaction(connector_name):
            await self._retry_market_cleanup_residual(connector_name, trading_pair)
            connector_preexisting = connector_name in self.connectors
            connector = self.connectors.get(connector_name)

            # A local marker without a ready book has ambiguous remote ownership. Until
            # child B/C returns an initialization transaction result, retrying it could
            # create a second subscription that cannot be attributed or rolled back.
            if trading_pair in self._markets.get(connector_name, set()):
                tracker = getattr(connector, "order_book_tracker", None)
                if connector is None or not self._order_book_is_ready(tracker, trading_pair):
                    self._raise_ambiguous_market_state(connector_name, trading_pair)

            if connector is not None:
                tracker = getattr(connector, "order_book_tracker", None)
                if self._pair_exists_in_connector_state(connector, tracker, trading_pair):
                    if self._order_book_is_ready(tracker, trading_pair):
                        self._markets.setdefault(connector_name, set()).add(trading_pair)
                        self._cleanup_residuals.pop((connector_name, trading_pair), None)
                        return
                    self._raise_ambiguous_market_state(connector_name, trading_pair)

            try:
                connector = await self.ensure_connector(connector_name)
                if connector is None:
                    connector = self.connectors.get(connector_name)
                if connector is None:
                    raise ValueError(f"Connector {connector_name} not available. Check credentials.")

                tracker = getattr(connector, "order_book_tracker", None)
                if self._pair_exists_in_connector_state(connector, tracker, trading_pair):
                    if self._order_book_is_ready(tracker, trading_pair):
                        self._markets.setdefault(connector_name, set()).add(trading_pair)
                        self._cleanup_residuals.pop((connector_name, trading_pair), None)
                        return
                    self._raise_ambiguous_market_state(connector_name, trading_pair)

                logger.info(f"Initializing order book for {connector_name}/{trading_pair}")
                success = await self._market_data_service.initialize_order_book(
                    connector_name=connector_name,
                    trading_pair=trading_pair,
                    account_name=self._account_name,
                    timeout=order_book_timeout,
                )
                if not success:
                    raise ValueError(
                        f"Failed to initialize order book for {connector_name}/{trading_pair}"
                    )

                self._register_trading_pair_with_connector(connector, trading_pair)
                if hasattr(connector, "_update_balances"):
                    await connector._update_balances()

                self._markets.setdefault(connector_name, set()).add(trading_pair)
                self._cleanup_residuals.pop((connector_name, trading_pair), None)
                logger.info(f"Market {connector_name}/{trading_pair} added to trading interface")
            except BaseException as primary_error:
                residual = await self._complete_market_cleanup(
                    connector_name,
                    trading_pair,
                    connector,
                    connector_preexisting,
                )
                if isinstance(primary_error, asyncio.CancelledError):
                    raise
                if residual is not None:
                    raise ConnectorCapabilityError(residual) from primary_error
                raise

    @asynccontextmanager
    async def _market_transaction(self, connector_name: str):
        """Serialize connector-wide mutations and prune the caller-controlled lock key."""
        async with self._market_locks_guard:
            lock = self._market_locks.setdefault(connector_name, asyncio.Lock())
            self._market_lock_users[connector_name] = self._market_lock_users.get(connector_name, 0) + 1
        acquired = False
        try:
            await lock.acquire()
            acquired = True
            yield
        finally:
            if acquired:
                lock.release()
            async with self._market_locks_guard:
                users = self._market_lock_users[connector_name] - 1
                if users:
                    self._market_lock_users[connector_name] = users
                else:
                    self._market_lock_users.pop(connector_name, None)
                    if self._market_locks.get(connector_name) is lock:
                        self._market_locks.pop(connector_name, None)

    @staticmethod
    def _order_book_is_ready(tracker, trading_pair: str) -> bool:
        if tracker is None or trading_pair not in getattr(tracker, "order_books", {}):
            return False
        try:
            bids, asks = tracker.order_books[trading_pair].snapshot
            return len(bids) > 0 and len(asks) > 0
        except Exception:
            return False

    @staticmethod
    def _pair_exists_in_connector_state(connector, tracker, trading_pair: str) -> bool:
        values = [getattr(connector, "_trading_pairs", ())]
        if tracker is not None:
            values.append(getattr(tracker, "_trading_pairs", ()))
            values.extend(
                getattr(tracker, attr, {})
                for attr in (
                    "_order_books",
                    "order_books",
                    "_tracking_message_queues",
                    "_saved_message_queues",
                    "_past_diffs_windows",
                    "_tracking_tasks",
                )
            )
        return any(trading_pair in value for value in values)

    @staticmethod
    def _raise_ambiguous_market_state(connector_name: str, trading_pair: str) -> None:
        raise ConnectorCapabilityError(
            CapabilityErrorDetail(
                code="ambiguous_market_state",
                message=(
                    f"Market state for '{connector_name}/{trading_pair}' is present but not ready"
                ),
                connector=connector_name,
                feature=ConnectorFeature.LIVE_ORDER_BOOK.value,
                trading_pair=trading_pair,
                reason=(
                    "remote initialization ownership is unknown until the accepted child B/C transaction seam"
                ),
            )
        )

    @staticmethod
    def _tasks_in_value(value) -> Set[asyncio.Task]:
        tasks: Set[asyncio.Task] = set()
        if isinstance(value, asyncio.Task):
            tasks.add(value)
        elif isinstance(value, dict):
            tasks.update(item for item in value.values() if isinstance(item, asyncio.Task))
        elif isinstance(value, (list, set, tuple)):
            tasks.update(item for item in value if isinstance(item, asyncio.Task))
        return tasks

    async def _remove_owned_pair_state(
        self,
        connector_name: str,
        trading_pair: str,
        connector,
    ) -> None:
        pairs = self._markets.get(connector_name)
        if pairs is not None:
            pairs.discard(trading_pair)
            if not pairs:
                self._markets.pop(connector_name, None)

        if connector is None:
            return
        tracker = getattr(connector, "order_book_tracker", None)
        tasks: Set[asyncio.Task] = set()
        for owner in (connector, tracker):
            if owner is None:
                continue
            pair_list = getattr(owner, "_trading_pairs", None)
            if isinstance(pair_list, list):
                pair_list[:] = [pair for pair in pair_list if pair != trading_pair]
            elif isinstance(pair_list, set):
                pair_list.discard(trading_pair)

        if tracker is not None:
            for attr in (
                "_order_books",
                "order_books",
                "_tracking_message_queues",
                "_saved_message_queues",
                "_past_diffs_windows",
                "_tracking_tasks",
            ):
                mapping = getattr(tracker, attr, None)
                if isinstance(mapping, dict):
                    tasks.update(self._tasks_in_value(mapping.pop(trading_pair, None)))
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _remote_cleanup_reason(
        self,
        connector_name: str,
        trading_pair: str,
        connector_preexisting: bool,
    ) -> Optional[str]:
        try:
            if connector_preexisting:
                cleanup = getattr(self._market_data_service, "remove_trading_pair", None)
                if cleanup is None:
                    return "public pair cleanup is unavailable"
                result = await asyncio.wait_for(
                    cleanup(
                        connector_name=connector_name,
                        trading_pair=trading_pair,
                        account_name=self._account_name,
                    ),
                    timeout=5.0,
                )
            else:
                cleanup = getattr(self._connector_service, "stop_trading_connector", None)
                if cleanup is None:
                    return "public connector cleanup is unavailable"
                result = await asyncio.wait_for(
                    cleanup(self._account_name, connector_name), timeout=5.0
                )
            if result is False:
                return "public cleanup returned false"
            return None
        except asyncio.TimeoutError:
            return "public cleanup timed out"
        except asyncio.CancelledError:
            return "public cleanup was cancelled"
        except BaseException as cleanup_error:
            return f"public cleanup raised {type(cleanup_error).__name__}: {cleanup_error}"

    async def _retry_market_cleanup_residual(
        self, connector_name: str, trading_pair: str
    ) -> None:
        """Retry known residual cleanup before permitting any new remote mutation."""
        key = (connector_name, trading_pair)
        residual = self._cleanup_residuals.get(key)
        if residual is None:
            return
        reason = await self._remote_cleanup_reason(
            connector_name,
            trading_pair,
            residual.get("cleanup_scope", "pair") == "pair",
        )
        if reason is None:
            self._cleanup_residuals.pop(key, None)
            return
        residual["reason"] = reason
        raise ConnectorCapabilityError(CapabilityErrorDetail.model_validate(residual))

    async def _finalize_failed_market_add(
        self,
        connector_name: str,
        trading_pair: str,
        connector,
        connector_preexisting: bool,
    ) -> Optional[CapabilityErrorDetail]:
        """Run bounded public cleanup, then remove only transaction-owned pair state."""
        reason = await self._remote_cleanup_reason(
            connector_name, trading_pair, connector_preexisting
        )
        try:
            await self._remove_owned_pair_state(connector_name, trading_pair, connector)
        except BaseException as local_error:
            local_reason = f"local cleanup raised {type(local_error).__name__}: {local_error}"
            reason = f"{reason}; {local_reason}" if reason else local_reason

        key = (connector_name, trading_pair)
        if reason is None:
            self._cleanup_residuals.pop(key, None)
            return None
        detail = CapabilityErrorDetail(
            code="market_cleanup_residual",
            message=f"Cleanup is incomplete for '{connector_name}/{trading_pair}'",
            connector=connector_name,
            feature=ConnectorFeature.LIVE_ORDER_BOOK.value,
            trading_pair=trading_pair,
            reason=reason,
        )
        residual = detail.model_dump(exclude_none=True)
        residual["cleanup_scope"] = "pair" if connector_preexisting else "connector"
        self._cleanup_residuals[key] = residual
        return detail

    async def _complete_market_cleanup(
        self,
        connector_name: str,
        trading_pair: str,
        connector,
        connector_preexisting: bool,
    ) -> Optional[CapabilityErrorDetail]:
        """Shield finalization from caller cancellation while preserving that cancellation."""
        cleanup_task = asyncio.create_task(
            self._finalize_failed_market_add(
                connector_name,
                trading_pair,
                connector,
                connector_preexisting,
            )
        )
        while not cleanup_task.done():
            try:
                await asyncio.shield(cleanup_task)
            except asyncio.CancelledError:
                continue
        return cleanup_task.result()

    async def remove_market(
        self,
        connector_name: str,
        trading_pair: str,
        remove_order_book: bool = True
    ):
        """
        Remove a trading pair from active markets.

        Args:
            connector_name: Name of the connector
            trading_pair: Trading pair to remove
            remove_order_book: Whether to remove the order book (default True)
        """
        if connector_name not in self._markets:
            return

        self._markets[connector_name].discard(trading_pair)
        if not self._markets[connector_name]:
            del self._markets[connector_name]

        # Remove order book via MarketDataService
        if remove_order_book:
            try:
                await self._market_data_service.remove_trading_pair(
                    connector_name=connector_name,
                    trading_pair=trading_pair,
                    account_name=self._account_name
                )
            except Exception as e:
                logger.warning(f"Failed to remove order book for {connector_name}/{trading_pair}: {e}")

        logger.info(f"Removed market {connector_name}/{trading_pair}")

    def _register_trading_pair_with_connector(
        self,
        connector: ConnectorBase,
        trading_pair: str
    ):
        """
        Register a trading pair with the connector's internal structures.

        Args:
            connector: The connector instance (ExchangePyBase)
            trading_pair: Trading pair to register
        """
        if trading_pair not in connector._trading_pairs:
            connector._trading_pairs.append(trading_pair)
            logger.debug(f"Registered {trading_pair} with connector {type(connector).__name__}")

    # ========================================
    # ScriptStrategyBase-compatible methods
    # These are called by executors via self._strategy.method()
    # ========================================

    def buy(
        self,
        connector_name: str,
        trading_pair: str,
        amount: Decimal,
        order_type: OrderType,
        price: Decimal = Decimal("NaN"),
        position_action: PositionAction = PositionAction.NIL
    ) -> str:
        """
        Place a buy order.

        Args:
            connector_name: Name of the connector
            trading_pair: Trading pair
            amount: Order amount in base currency
            order_type: Type of order (LIMIT, MARKET, etc.)
            price: Order price (for limit orders)
            position_action: Position action for perpetuals

        Returns:
            Client order ID
        """
        connector = self.connectors.get(connector_name)
        if not connector:
            raise ValueError(f"Connector {connector_name} not loaded. Call ensure_connector first.")

        return connector.buy(
            trading_pair=trading_pair,
            amount=amount,
            order_type=order_type,
            price=price,
            position_action=position_action
        )

    def sell(
        self,
        connector_name: str,
        trading_pair: str,
        amount: Decimal,
        order_type: OrderType,
        price: Decimal = Decimal("NaN"),
        position_action: PositionAction = PositionAction.NIL
    ) -> str:
        """
        Place a sell order.

        Args:
            connector_name: Name of the connector
            trading_pair: Trading pair
            amount: Order amount in base currency
            order_type: Type of order (LIMIT, MARKET, etc.)
            price: Order price (for limit orders)
            position_action: Position action for perpetuals

        Returns:
            Client order ID
        """
        connector = self.connectors.get(connector_name)
        if not connector:
            raise ValueError(f"Connector {connector_name} not loaded. Call ensure_connector first.")

        return connector.sell(
            trading_pair=trading_pair,
            amount=amount,
            order_type=order_type,
            price=price,
            position_action=position_action
        )

    def cancel(
        self,
        connector_name: str,
        trading_pair: str,
        order_id: str
    ) -> str:
        """
        Cancel an order.

        Args:
            connector_name: Name of the connector
            trading_pair: Trading pair
            order_id: Client order ID to cancel

        Returns:
            Client order ID that was cancelled
        """
        connector = self.connectors.get(connector_name)
        if not connector:
            raise ValueError(f"Connector {connector_name} not loaded. Call ensure_connector first.")

        return connector.cancel(trading_pair=trading_pair, client_order_id=order_id)

    def get_active_orders(self, connector_name: str) -> List:
        """
        Get active orders for a connector.

        Args:
            connector_name: Name of the connector

        Returns:
            List of active in-flight orders
        """
        connector = self.connectors.get(connector_name)
        if not connector:
            return []
        return list(connector.in_flight_orders.values())

    # ========================================
    # Additional helper methods
    # ========================================

    def get_connector(self, connector_name: str) -> Optional[ConnectorBase]:
        """
        Get a connector by name.

        Args:
            connector_name: Name of the connector

        Returns:
            The connector instance or None if not loaded
        """
        return self.connectors.get(connector_name)

    def is_connector_loaded(self, connector_name: str) -> bool:
        """
        Check if a connector is loaded.

        Args:
            connector_name: Name of the connector

        Returns:
            True if connector is loaded
        """
        return connector_name in self.connectors

    def get_all_trading_pairs(self) -> Dict[str, Set[str]]:
        """
        Get all active trading pairs by connector.

        Returns:
            Dictionary mapping connector names to sets of trading pairs
        """
        return {k: v.copy() for k, v in self._markets.items()}

    async def cleanup(self):
        """
        Cleanup resources. Called when shutting down.
        """
        self._markets.clear()
        logger.info(f"AccountTradingInterface cleanup completed for account {self._account_name}")


class TradingService:
    """
    Centralized trading service using UnifiedConnectorService.

    This service manages trading interfaces for each account (executor-compatible).
    """

    def __init__(
        self,
        connector_service: "UnifiedConnectorService",
        market_data_service: "MarketDataService"
    ):
        """
        Initialize the TradingService.

        Args:
            connector_service: UnifiedConnectorService for connector access
            market_data_service: MarketDataService for order book operations
        """
        self._connector_service = connector_service
        self._market_data_service = market_data_service

        # Trading interfaces per account (for executor use)
        self._trading_interfaces: Dict[str, AccountTradingInterface] = {}

        logger.info("TradingService initialized")

    # ==================== Trading Interface ====================

    def get_trading_interface(self, account_name: str) -> AccountTradingInterface:
        """
        Get or create a trading interface for the specified account.

        This interface provides ScriptStrategyBase-compatible methods
        that executors can use for trading operations.

        Args:
            account_name: Account to get trading interface for

        Returns:
            AccountTradingInterface instance for the account
        """
        if account_name not in self._trading_interfaces:
            self._trading_interfaces[account_name] = AccountTradingInterface(
                connector_service=self._connector_service,
                market_data_service=self._market_data_service,
                account_name=account_name
            )
        return self._trading_interfaces[account_name]

    def get_all_trading_interfaces(self) -> Dict[str, AccountTradingInterface]:
        """Get all active trading interfaces."""
        return self._trading_interfaces.copy()

    # ==================== Lifecycle ====================

    async def stop(self):
        """Stop all trading interfaces and cleanup resources."""
        logger.info("Stopping TradingService...")

        for account_name, interface in self._trading_interfaces.items():
            try:
                await interface.cleanup()
            except Exception as e:
                logger.error(f"Error cleaning up interface for {account_name}: {e}")

        self._trading_interfaces.clear()
        logger.info("TradingService stopped")

    def update_all_timestamps(self):
        """Update timestamps for all trading interfaces. Called by executor control loop."""
        for interface in self._trading_interfaces.values():
            interface.update_timestamp()

    # ==================== Properties ====================

    @property
    def connector_service(self) -> "UnifiedConnectorService":
        """Get the connector service instance."""
        return self._connector_service

    @property
    def market_data_service(self) -> "MarketDataService":
        """Get the market data service instance."""
        return self._market_data_service
