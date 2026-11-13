"""
Trading Service - Centralized trading operations with executor-compatible interface.

This service provides trading operations (buy, sell, cancel) using the
UnifiedConnectorService for connector management.
"""
import asyncio
import logging
import time
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Tuple

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import OrderType, PositionAction
from services.connector_capabilities import ConnectorFeature

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
        self._market_locks: Dict[Tuple[str, str], asyncio.Lock] = {}

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

        lock = self._market_locks.setdefault((connector_name, trading_pair), asyncio.Lock())
        async with lock:
            markets_snapshot = {name: set(pairs) for name, pairs in self._markets.items()}
            connector_preexisting = connector_name in self.connectors
            connector = None
            state_snapshot = None
            pair_preexisting = False
            try:
                connector = await self.ensure_connector(connector_name)
                if connector is None:
                    connector = self.connectors.get(connector_name)
                if connector is None:
                    raise ValueError(f"Connector {connector_name} not available. Check credentials.")

                tracker = getattr(connector, "order_book_tracker", None)
                pair_preexisting = self._pair_exists_in_connector_state(
                    connector, tracker, trading_pair
                )
                state_snapshot = self._snapshot_connector_state(connector, tracker)

                if trading_pair in self._markets.get(connector_name, set()):
                    if self._order_book_is_ready(tracker, trading_pair):
                        logger.debug(
                            f"Market {connector_name}/{trading_pair} already active with valid order book"
                        )
                        return
                    logger.info(
                        f"Market {connector_name}/{trading_pair} tracked but order book not ready, re-initializing"
                    )

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
                logger.info(f"Market {connector_name}/{trading_pair} added to trading interface")
            except BaseException:
                await self._rollback_market_add(
                    connector_name=connector_name,
                    trading_pair=trading_pair,
                    connector=connector,
                    state_snapshot=state_snapshot,
                    markets_snapshot=markets_snapshot,
                    connector_preexisting=connector_preexisting,
                    pair_preexisting=pair_preexisting,
                )
                raise

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
        return any(
            trading_pair in value
            for value in (
                getattr(connector, "_trading_pairs", ()),
                getattr(tracker, "_trading_pairs", ()) if tracker is not None else (),
                getattr(tracker, "order_books", {}) if tracker is not None else {},
            )
        )

    @staticmethod
    def _snapshot_connector_state(connector, tracker) -> Dict[str, Any]:
        """Snapshot only mutable connector/tracker structures touched by initialization."""
        entries = []
        owners_and_attrs = [(connector, "_trading_pairs")]
        if tracker is not None:
            owners_and_attrs.extend(
                (tracker, attr)
                for attr in (
                    "_trading_pairs",
                    "_order_books",
                    "order_books",
                    "_tracking_message_queues",
                    "_saved_message_queues",
                    "_past_diffs_windows",
                    "_tracking_tasks",
                    "_tasks_to_drain",
                    "_order_book_diff_stream",
                    "_order_book_snapshot_stream",
                    "_order_book_trade_stream",
                    "_generation",
                    "_active_generation",
                    "_init_order_books_task",
                    "_emit_trade_event_task",
                    "_order_book_diff_listener_task",
                    "_order_book_trade_listener_task",
                    "_order_book_snapshot_listener_task",
                    "_order_book_diff_router_task",
                    "_order_book_snapshot_router_task",
                    "_update_last_trade_prices_task",
                    "_order_book_stream_listener_task",
                )
            )

        seen = set()
        for owner, attr in owners_and_attrs:
            if owner is None or not hasattr(owner, attr):
                continue
            value = getattr(owner, attr)
            marker = (id(owner), attr)
            if marker in seen:
                continue
            seen.add(marker)
            if isinstance(value, dict):
                saved = dict(value)
                kind = "dict"
            elif isinstance(value, list):
                saved = list(value)
                kind = "list"
            elif isinstance(value, set):
                saved = set(value)
                kind = "set"
            else:
                saved = value
                kind = "value"
            entries.append((owner, attr, value, kind, saved))

        event = getattr(tracker, "_order_books_initialized", None) if tracker is not None else None
        return {
            "entries": entries,
            "event": event,
            "event_set": event.is_set() if event is not None else None,
        }

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

    async def _rollback_market_add(
        self,
        connector_name: str,
        trading_pair: str,
        connector,
        state_snapshot: Optional[Dict[str, Any]],
        markets_snapshot: Dict[str, Set[str]],
        connector_preexisting: bool,
        pair_preexisting: bool,
    ) -> None:
        """Restore API-owned local state and drain tasks created by this transaction."""
        self._markets.clear()
        self._markets.update({name: set(pairs) for name, pairs in markets_snapshot.items()})

        if connector is None:
            return
        tracker = getattr(connector, "order_book_tracker", None)
        prior_tasks: Set[asyncio.Task] = set()
        current_tasks: Set[asyncio.Task] = set()
        if state_snapshot is not None:
            for owner, attr, original, _, saved in state_snapshot["entries"]:
                prior_tasks.update(self._tasks_in_value(saved))
                current_tasks.update(
                    self._tasks_in_value(getattr(owner, attr, original))
                )

        candidate_tasks = current_tasks - prior_tasks
        for task in candidate_tasks:
            if not task.done():
                task.cancel()
        if candidate_tasks:
            await asyncio.gather(*candidate_tasks, return_exceptions=True)

        if not connector_preexisting:
            stop = getattr(self._connector_service, "stop_trading_connector", None)
            if stop is not None:
                try:
                    await stop(self._account_name, connector_name)
                except Exception as cleanup_error:
                    logger.warning(
                        "Connector cleanup failed for %s: %s; restoring local state only",
                        connector_name,
                        cleanup_error,
                    )
        elif not pair_preexisting:
            remove = getattr(self._market_data_service, "remove_trading_pair", None)
            if remove is not None:
                try:
                    removed = await remove(
                        connector_name=connector_name,
                        trading_pair=trading_pair,
                        account_name=self._account_name,
                    )
                    if not removed:
                        logger.warning(
                            "Remote pair cleanup was unavailable for %s/%s; restoring local state only",
                            connector_name,
                            trading_pair,
                        )
                except Exception as cleanup_error:
                    logger.warning(
                        "Remote pair cleanup failed for %s/%s: %s; restoring local state only",
                        connector_name,
                        trading_pair,
                        cleanup_error,
                    )

        if state_snapshot is None:
            return
        for owner, attr, original, kind, saved in state_snapshot["entries"]:
            if kind == "dict":
                original.clear()
                original.update(saved)
            elif kind == "list":
                original[:] = saved
            elif kind == "set":
                original.clear()
                original.update(saved)
            try:
                setattr(owner, attr, original)
            except (AttributeError, TypeError):
                pass

        event = state_snapshot["event"]
        if event is not None:
            event.set() if state_snapshot["event_set"] else event.clear()

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
