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
from weakref import WeakKeyDictionary

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import OrderType, PositionAction
from models.connectors import CapabilityErrorDetail
from services.connector_capabilities import (
    ConnectorCapabilityError,
    ConnectorCapabilityRegistry,
    ConnectorFeature,
)

if TYPE_CHECKING:
    from services.market_data_service import MarketDataService
    from services.unified_connector_service import UnifiedConnectorService


logger = logging.getLogger(__name__)

_TASK_CLEANUP_TIMEOUT = 0.01
_REMOTE_CLEANUP_TIMEOUT = 0.05
_CONNECTOR_MARKET_PAIR_OWNERS = WeakKeyDictionary()


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
        # Connector-wide transaction and cleanup authority belongs to UCS. Every
        # interface for the same account/connector therefore shares the same key.
        if not hasattr(connector_service, "_market_locks"):
            connector_service._market_locks = {}
        if not hasattr(connector_service, "_market_lock_users"):
            connector_service._market_lock_users = {}
        if not hasattr(connector_service, "_market_locks_guard"):
            connector_service._market_locks_guard = asyncio.Lock()
        if not hasattr(connector_service, "_market_pair_owners"):
            connector_service._market_pair_owners = {}
        if not hasattr(connector_service, "_market_cleanup_residuals"):
            connector_service._market_cleanup_residuals = {}
        if not hasattr(connector_service, "_market_cleanup_owners"):
            connector_service._market_cleanup_owners = {}
        if not hasattr(connector_service, "_market_residual_tasks"):
            connector_service._market_residual_tasks = {}
        if not hasattr(connector_service, "_market_cleanup_phases"):
            connector_service._market_cleanup_phases = {}
        self._market_locks = connector_service._market_locks
        self._market_lock_users = connector_service._market_lock_users
        self._market_locks_guard = connector_service._market_locks_guard
        self._market_pair_owners = connector_service._market_pair_owners
        self._cleanup_residuals = connector_service._market_cleanup_residuals
        self._cleanup_owners = connector_service._market_cleanup_owners
        self._residual_tasks = connector_service._market_residual_tasks
        self._cleanup_phases = connector_service._market_cleanup_phases
        self._market_owner_token = object()
        self._capability_registry = ConnectorCapabilityRegistry()

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
        return {
            (value["connector"], value.get("trading_pair")): dict(value)
            for key, value in self._cleanup_residuals.items()
            if key[0] == self._account_name
        }

    def _residual_key(
        self, connector_name: str, trading_pair: str, cleanup_scope: str
    ) -> Tuple[str, ...]:
        """Keep connector-owned failures under connector identity, never pair identity."""
        if cleanup_scope == "connector":
            return (self._account_name, connector_name, "connector")
        return (self._account_name, connector_name, "pair", trading_pair)

    def _clear_pair_residual(self, connector_name: str, trading_pair: str) -> None:
        key = self._residual_key(connector_name, trading_pair, "pair")
        self._cleanup_residuals.pop(key, None)
        self._cleanup_owners.pop(key, None)

    def _pair_owner_key(
        self, connector_name: str, trading_pair: str
    ) -> Tuple[str, str, str]:
        return (self._account_name, connector_name, trading_pair)

    def _register_market_owner(
        self, connector_name: str, trading_pair: str
    ) -> None:
        key = self._pair_owner_key(connector_name, trading_pair)
        owners = self._market_owner_set(connector_name, trading_pair, create=True)
        owners.add(self._market_owner_token)

    def _market_owner_set(
        self,
        connector_name: str,
        trading_pair: str,
        *,
        create: bool = False,
    ) -> Set[object]:
        """Resolve the token set from the shared connector, with a UCS mirror."""
        key = self._pair_owner_key(connector_name, trading_pair)
        connector = self.connectors.get(connector_name)
        if connector is not None:
            try:
                ledger = _CONNECTOR_MARKET_PAIR_OWNERS.get(connector)
                if ledger is None and create:
                    ledger = {}
                    _CONNECTOR_MARKET_PAIR_OWNERS[connector] = ledger
            except TypeError:
                # Some compiled connector classes are not weak-referenceable. Their
                # interfaces still share the UCS-owned mirror used below.
                ledger = None
            if ledger is not None:
                owners = ledger.get(key)
                if owners is None and create:
                    owners = ledger.setdefault(key, set())
                if owners is not None:
                    self._market_pair_owners[key] = owners
                    return owners
        if create:
            return self._market_pair_owners.setdefault(key, set())
        return self._market_pair_owners.get(key, set())

    def _unregister_market_owner(
        self, connector_name: str, trading_pair: str
    ) -> None:
        key = self._pair_owner_key(connector_name, trading_pair)
        owners = self._market_owner_set(connector_name, trading_pair)
        if not owners:
            return
        owners.discard(self._market_owner_token)
        if not owners:
            self._market_pair_owners.pop(key, None)
            connector = self.connectors.get(connector_name)
            if connector is not None:
                try:
                    ledger = _CONNECTOR_MARKET_PAIR_OWNERS.get(connector)
                    if ledger is not None:
                        ledger.pop(key, None)
                        if not ledger:
                            _CONNECTOR_MARKET_PAIR_OWNERS.pop(connector, None)
                except TypeError:
                    pass

    def _owns_market_pair(self, connector_name: str, trading_pair: str) -> bool:
        """Return whether this interface owns the exact account/connector/pair token."""
        owners = self._market_owner_set(connector_name, trading_pair)
        return self._market_owner_token in owners

    def _has_other_market_owners(
        self, connector_name: str, trading_pair: str
    ) -> bool:
        owners = self._market_owner_set(connector_name, trading_pair)
        return bool(owners - {self._market_owner_token})

    def _remove_local_market_ownership(
        self, connector_name: str, trading_pair: str
    ) -> None:
        pairs = self._markets.get(connector_name)
        if pairs is not None:
            pairs.discard(trading_pair)
            if not pairs:
                self._markets.pop(connector_name, None)
        self._unregister_market_owner(connector_name, trading_pair)

    def _other_owned_pairs(
        self, connector_name: str, trading_pair: str
    ) -> Set[str]:
        connector = self.connectors.get(connector_name)
        try:
            ledger = (
                _CONNECTOR_MARKET_PAIR_OWNERS.get(connector)
                if connector is not None else None
            )
        except TypeError:
            ledger = None
        owner_items = (
            ledger.items() if isinstance(ledger, dict)
            else self._market_pair_owners.items()
        )
        return {
            pair
            for (account, connector, pair), owners in owner_items
            if account == self._account_name
            and connector == connector_name
            and pair != trading_pair
            and owners
        }

    @property
    def current_timestamp(self) -> float:
        """Return current timestamp (updated by control loop)."""
        return self._current_timestamp

    def update_timestamp(self):
        """Update the current timestamp. Called by ExecutorService control loop."""
        self._current_timestamp = time.time()

    def _require_capability(
        self,
        connector_name: str,
        feature: ConnectorFeature,
        trading_pair: Optional[str] = None,
    ) -> None:
        """Use the shared declaration seam before connector access or mutation."""
        require = getattr(self._connector_service, "require_capability", None)
        if require is None:
            require = getattr(self._market_data_service, "require_capability", None)
        if require is not None:
            require(connector_name, feature, trading_pair)
            return
        self._capability_registry.require(connector_name, feature, trading_pair)

    async def ensure_connector(self, connector_name: str) -> ConnectorBase:
        """
        Ensure connector is loaded and available.

        Args:
            connector_name: Name of the connector

        Returns:
            The connector instance
        """
        self._require_capability(connector_name, ConnectorFeature.ORDER_SUBMISSION)
        return await self._connector_service.get_trading_connector(
            self._account_name,
            connector_name
        )

    def _connector_authority(
        self, connector_name: str, connector: ConnectorBase
    ) -> Tuple[ConnectorBase, Optional[int]]:
        """Snapshot the physical connector and its published incarnation."""
        incarnation = getattr(
            self._connector_service, "_connector_incarnation", None
        )
        token = (
            incarnation((self._account_name, connector_name))
            if callable(incarnation)
            else None
        )
        return connector, token

    def _require_current_connector_authority(
        self,
        connector_name: str,
        authority: Tuple[ConnectorBase, Optional[int]],
    ) -> None:
        """Reject a local commit when physical connector ownership changed."""
        connector, incarnation = authority
        current_incarnation = getattr(
            self._connector_service, "_connector_incarnation", None
        )
        if self.connectors.get(connector_name) is connector and (
            not callable(current_incarnation)
            or current_incarnation((self._account_name, connector_name))
            == incarnation
        ):
            return
        raise ConnectorCapabilityError(
            CapabilityErrorDetail(
                code="connector_incarnation_changed",
                message=(
                    f"Connector '{connector_name}' changed while adding a market"
                ),
                connector=connector_name,
                feature=ConnectorFeature.LIVE_ORDER_BOOK.value,
                reason="connector_incarnation_changed",
            )
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
        # All checks are local declarations. They must run before ensure_connector(),
        # which can authenticate, create a connector, and start network tasks.
        self._require_capability(
            connector_name, ConnectorFeature.DYNAMIC_SUBSCRIPTION, trading_pair
        )
        self._require_capability(
            connector_name, ConnectorFeature.LIVE_ORDER_BOOK, trading_pair
        )
        self._require_capability(
            connector_name, ConnectorFeature.ORDER_SUBMISSION, trading_pair
        )

        async with self._market_transaction(connector_name):
            await self._retry_market_cleanup_residual(connector_name, trading_pair)
            connector_preexisting = connector_name in self.connectors
            connector = self.connectors.get(connector_name)
            authority = (
                self._connector_authority(connector_name, connector)
                if connector is not None
                else None
            )

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
                        self._require_current_connector_authority(
                            connector_name, authority
                        )
                        self._markets.setdefault(connector_name, set()).add(trading_pair)
                        self._register_market_owner(connector_name, trading_pair)
                        self._clear_pair_residual(connector_name, trading_pair)
                        return
                    self._raise_ambiguous_market_state(connector_name, trading_pair)

            try:
                connector = await self.ensure_connector(connector_name)
                if connector is None:
                    connector = self.connectors.get(connector_name)
                if connector is None:
                    raise ValueError(f"Connector {connector_name} not available. Check credentials.")
                authority = self._connector_authority(connector_name, connector)

                tracker = getattr(connector, "order_book_tracker", None)
                if self._pair_exists_in_connector_state(connector, tracker, trading_pair):
                    if self._order_book_is_ready(tracker, trading_pair):
                        self._require_current_connector_authority(
                            connector_name, authority
                        )
                        self._markets.setdefault(connector_name, set()).add(trading_pair)
                        self._register_market_owner(connector_name, trading_pair)
                        self._clear_pair_residual(connector_name, trading_pair)
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

                self._require_current_connector_authority(
                    connector_name, authority
                )
                self._markets.setdefault(connector_name, set()).add(trading_pair)
                self._register_market_owner(connector_name, trading_pair)
                self._clear_pair_residual(connector_name, trading_pair)
                logger.info(f"Market {connector_name}/{trading_pair} added to trading interface")
            except BaseException as primary_error:
                residual = await self._complete_market_cleanup(
                    connector_name,
                    trading_pair,
                    connector,
                    connector_preexisting,
                    authority[1] if authority is not None else None,
                )
                if isinstance(primary_error, asyncio.CancelledError):
                    raise
                if residual is not None:
                    raise ConnectorCapabilityError(residual) from primary_error
                raise

    @asynccontextmanager
    async def _market_transaction(self, connector_name: str):
        """Serialize connector-wide mutations and prune the caller-controlled lock key."""
        transaction_key = (self._account_name, connector_name)
        shared_transaction = getattr(
            self._connector_service, "_market_transaction_lock", None
        )
        if callable(shared_transaction):
            async with shared_transaction(transaction_key):
                yield
            return
        async with self._market_locks_guard:
            lock = self._market_locks.setdefault(transaction_key, asyncio.Lock())
            self._market_lock_users[transaction_key] = self._market_lock_users.get(transaction_key, 0) + 1
        acquired = False
        try:
            await lock.acquire()
            acquired = True
            yield
        finally:
            if acquired:
                lock.release()
            try:
                # Preserve a queued caller cancellation after the connector lock
                # is released. The nested finally keeps map pruning atomic even
                # when this checkpoint raises CancelledError repeatedly.
                await asyncio.sleep(0)
            finally:
                users = self._market_lock_users.get(transaction_key, 1) - 1
                if users:
                    self._market_lock_users[transaction_key] = users
                else:
                    self._market_lock_users.pop(transaction_key, None)
                    if self._market_locks.get(transaction_key) is lock:
                        self._market_locks.pop(transaction_key, None)

    def _retain_market_transaction_descendant(
        self, connector_name: str, delegate_task: asyncio.Task
    ) -> Optional[bool]:
        """Retain a physical child under the shared market-lock lease."""
        retain = getattr(
            self._connector_service,
            "_retain_market_transaction_descendant",
            None,
        )
        if not callable(retain):
            return None
        return bool(
            retain((self._account_name, connector_name), delegate_task)
        )

    def _market_transaction_has_live_descendant(
        self, connector_name: str
    ) -> bool:
        has_live_descendant = getattr(
            self._connector_service,
            "_market_transaction_has_live_descendant",
            None,
        )
        return bool(
            callable(has_live_descendant)
            and has_live_descendant((self._account_name, connector_name))
        )

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

    async def _drain_tasks(
        self,
        key: Tuple[str, ...],
        tasks: Set[asyncio.Task],
    ) -> bool:
        """Cancel tasks with a real deadline and retain cancellation suppressors."""
        current = asyncio.current_task()
        new_tasks = set(tasks)
        retained_tasks = self._residual_tasks.pop(key, set())
        owned = new_tasks | retained_tasks
        pending_tasks = {
            task for task in owned if task is not current and not task.done()
        }
        # A retained task already received cancellation. Retry observes its
        # terminal state without a second cancellation that could hide a
        # cancellation-suppressing cleanup owner.
        for task in pending_tasks & new_tasks:
            if task.cancelling() == 0:
                task.cancel()
        if not pending_tasks:
            return True
        done, pending = await asyncio.wait(
            pending_tasks, timeout=_TASK_CLEANUP_TIMEOUT
        )
        for task in done:
            try:
                task.exception()
            except (asyncio.CancelledError, Exception):
                pass
        if pending:
            self._residual_tasks[key] = set(pending)
            return False
        return True

    async def _remove_owned_pair_state(
        self,
        connector_name: str,
        trading_pair: str,
        connector,
    ) -> bool:
        self._remove_local_market_ownership(connector_name, trading_pair)

        task_key = (
            self._account_name,
            connector_name,
            "pair_tasks",
            trading_pair,
        )

        # Pair state belongs to every token for this exact account/connector/pair.
        # A failed transaction or released interface may consume only its own local
        # view while another interface still owns the shared connector/tracker book.
        if self._market_owner_set(connector_name, trading_pair):
            return await self._drain_tasks(task_key, set())

        if connector is None:
            return await self._drain_tasks(task_key, set())
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
        return await self._drain_tasks(task_key, tasks)

    async def _drain_market_residual_tasks(
        self, connector_name: str, trading_pair: str
    ) -> bool:
        """Consume every retained task handle owned by this account/pair."""
        keys = [
            key
            for key in self._residual_tasks
            if len(key) >= 4
            and key[0] == self._account_name
            and key[1] == connector_name
            and key[-1] == trading_pair
        ]
        complete = True
        for key in keys:
            if not await self._drain_tasks(key, set()):
                complete = False
        return complete

    async def _remote_cleanup_reason(
        self,
        connector_name: str,
        trading_pair: str,
        connector_preexisting: bool,
    ) -> Optional[str]:
        task_key = (
            self._account_name,
            connector_name,
            "remote_cleanup",
            trading_pair,
        )
        phase = self._cleanup_phases.get(task_key)
        if (
            phase is not None
            and phase["complete"]
            and not phase["was_retained"]
            and phase["error_code"] is not None
        ):
            # An immediate false/raise is retryable on a later public call. A
            # retained timeout owner is different: its terminal result is the
            # authoritative operation result and must never be repeated.
            self._cleanup_phases.pop(task_key, None)
            phase = None
        if phase is None:
            phase = {
                "task": None,
                "complete": False,
                "result": None,
                "error_code": None,
                "cancel_requested": False,
                "was_retained": False,
                "connector_preexisting": connector_preexisting,
            }
            self._cleanup_phases[task_key] = phase
        if phase["complete"]:
            return phase["error_code"]

        try:
            cleanup_task = phase["task"]
            if cleanup_task is None:
                if connector_preexisting:
                    cleanup = getattr(
                        self._market_data_service, "remove_trading_pair", None
                    )
                    if cleanup is None:
                        phase["complete"] = True
                        phase["error_code"] = "pair_cleanup_unavailable"
                        return phase["error_code"]
                    awaitable = cleanup(
                        connector_name=connector_name,
                        trading_pair=trading_pair,
                        account_name=self._account_name,
                    )
                else:
                    cleanup = getattr(
                        self._connector_service,
                        "_stop_trading_connector_already_locked",
                        None,
                    )
                    cleanup_is_internal = cleanup is not None
                    if cleanup is None:
                        cleanup = getattr(
                            self._connector_service,
                            "stop_trading_connector",
                            None,
                        )
                    if cleanup is None:
                        phase["complete"] = True
                        phase["error_code"] = "connector_cleanup_unavailable"
                        return phase["error_code"]

                    async def stop_new_connector():
                        result = await cleanup(
                            self._account_name, connector_name
                        )
                        if cleanup_is_internal:
                            if result is not None:
                                raise ConnectorCapabilityError(result)
                            return True
                        return result

                    awaitable = stop_new_connector()
                cleanup_task = asyncio.ensure_future(awaitable)
                phase["task"] = cleanup_task
                retained = self._retain_market_transaction_descendant(
                    connector_name, cleanup_task
                )
                if retained is False:
                    cleanup_task.cancel()
                    await asyncio.gather(
                        cleanup_task, return_exceptions=True
                    )
                    phase["task"] = None
                    phase["complete"] = True
                    phase["error_code"] = "cleanup_authority_unavailable"
                    return phase["error_code"]

            if not cleanup_task.done():
                _, pending = await asyncio.wait(
                    {cleanup_task}, timeout=_REMOTE_CLEANUP_TIMEOUT
                )
            else:
                pending = set()
            if pending:
                if (
                    not phase["cancel_requested"]
                    and cleanup_task.cancelling() == 0
                ):
                    cleanup_task.cancel()
                    phase["cancel_requested"] = True
                phase["was_retained"] = True
                self._residual_tasks.setdefault(task_key, set()).add(
                    cleanup_task
                )
                return "cleanup_deadline_exceeded"

            retained = self._residual_tasks.get(task_key)
            if retained is not None:
                retained.discard(cleanup_task)
                if not retained:
                    self._residual_tasks.pop(task_key, None)
            try:
                result = cleanup_task.result()
            except asyncio.CancelledError:
                phase["complete"] = True
                phase["error_code"] = "cleanup_cancelled"
                return phase["error_code"]
            except BaseException:
                phase["complete"] = True
                phase["error_code"] = "cleanup_raised"
                return phase["error_code"]
            phase["complete"] = True
            phase["result"] = result
            if result is False:
                phase["error_code"] = "cleanup_returned_false"
                return phase["error_code"]
            return None
        except asyncio.CancelledError:
            cleanup_task = phase.get("task")
            if cleanup_task is not None:
                phase["was_retained"] = True
                self._residual_tasks.setdefault(task_key, set()).add(
                    cleanup_task
                )
            raise
        except BaseException:
            phase["complete"] = True
            phase["error_code"] = "cleanup_raised"
            return phase["error_code"]

    def _finish_remote_cleanup_phase(
        self, connector_name: str, trading_pair: str
    ) -> None:
        task_key = (
            self._account_name,
            connector_name,
            "remote_cleanup",
            trading_pair,
        )
        self._cleanup_phases.pop(task_key, None)
        self._residual_tasks.pop(task_key, None)

    async def _retry_market_cleanup_residual(
        self, connector_name: str, trading_pair: str
    ) -> None:
        """Retry known residual cleanup before permitting any new remote mutation."""
        pair_key = self._residual_key(connector_name, trading_pair, "pair")
        connector_key = self._residual_key(
            connector_name, trading_pair, "connector"
        )
        key = pair_key
        residual = self._cleanup_residuals.get(pair_key)
        connector_residual = self._cleanup_residuals.get(connector_key)
        if residual is None and connector_residual is not None:
            key = connector_key
            residual = connector_residual
        if residual is None:
            return
        residual_pair = residual.get("trading_pair") or trading_pair
        owner = self._cleanup_owners.get(key, {})
        connector = owner.get("connector") or self.connectors.get(connector_name)
        scope = residual.get("cleanup_scope", "pair")
        same_pair_owned = bool(
            self._market_owner_set(connector_name, residual_pair)
        )
        other_pairs = self._other_owned_pairs(connector_name, residual_pair)
        if same_pair_owned:
            # Shared state now belongs to a live peer token. Consuming this
            # transaction's residue must not unsubscribe or erase that peer.
            reason = None
        elif scope == "connector" and other_pairs:
            # A later pair now has independent ownership. Never stop its connector
            # while retrying the earlier transaction's residue.
            reason = None
        else:
            reason = await self._remote_cleanup_reason(
                connector_name,
                residual_pair,
                scope == "pair",
            )
        tasks_ok = False
        local_ok = False
        if not self._market_transaction_has_live_descendant(connector_name):
            tasks_ok = await self._drain_market_residual_tasks(
                connector_name, residual_pair
            )
            if tasks_ok:
                try:
                    local_ok = await self._remove_owned_pair_state(
                        connector_name, residual_pair, connector
                    )
                except asyncio.CancelledError:
                    raise
                except BaseException:
                    local_ok = False
        stale_local = (
            self._pair_exists_in_connector_state(
                connector,
                getattr(connector, "order_book_tracker", None),
                residual_pair,
            )
            if connector is not None and not same_pair_owned
            else False
        )
        if reason is None and tasks_ok and local_ok and not stale_local:
            self._finish_remote_cleanup_phase(connector_name, residual_pair)
            self._cleanup_residuals.pop(key, None)
            self._cleanup_owners.pop(key, None)
            return
        residual["reason"] = "cleanup_incomplete"
        residual["cleanup_code"] = reason or (
            "task_cleanup_pending" if not local_ok else "local_state_remaining"
        )
        raise ConnectorCapabilityError(CapabilityErrorDetail.model_validate(residual))

    async def _finalize_failed_market_add(
        self,
        connector_name: str,
        trading_pair: str,
        connector,
        connector_preexisting: bool,
        connector_incarnation=None,
    ) -> Optional[CapabilityErrorDetail]:
        """Run bounded public cleanup, then remove only transaction-owned pair state."""
        same_pair_owned = self._has_other_market_owners(
            connector_name, trading_pair
        )
        reason = (
            None
            if same_pair_owned
            else await self._remote_cleanup_reason(
                connector_name, trading_pair, connector_preexisting
            )
        )
        local_ok = False
        if not self._market_transaction_has_live_descendant(connector_name):
            try:
                local_ok = await self._remove_owned_pair_state(
                    connector_name, trading_pair, connector
                )
            except BaseException:
                local_ok = False

        stale_local = (
            self._pair_exists_in_connector_state(
                connector,
                getattr(connector, "order_book_tracker", None),
                trading_pair,
            )
            if connector is not None and not same_pair_owned
            else False
        )

        key = self._residual_key(
            connector_name,
            trading_pair,
            "pair" if connector_preexisting else "connector",
        )
        if reason is None and local_ok and not stale_local:
            self._finish_remote_cleanup_phase(connector_name, trading_pair)
            self._cleanup_residuals.pop(key, None)
            self._cleanup_owners.pop(key, None)
            return None
        cleanup_code = reason or (
            "task_cleanup_pending" if not local_ok else "local_state_remaining"
        )
        detail = CapabilityErrorDetail(
            code="market_cleanup_residual",
            message=f"Cleanup is incomplete for '{connector_name}/{trading_pair}'",
            connector=connector_name,
            feature=ConnectorFeature.LIVE_ORDER_BOOK.value,
            trading_pair=trading_pair,
            reason="cleanup_incomplete",
            cleanup_scope="pair" if connector_preexisting else "connector",
            cleanup_code=cleanup_code,
        )
        residual = detail.model_dump(exclude_none=True)
        self._cleanup_residuals[key] = residual
        self._cleanup_owners[key] = {
            "connector": connector,
            "connector_preexisting": connector_preexisting,
            "incarnation": connector_incarnation,
        }
        return detail

    async def _complete_market_cleanup(
        self,
        connector_name: str,
        trading_pair: str,
        connector,
        connector_preexisting: bool,
        connector_incarnation=None,
    ) -> Optional[CapabilityErrorDetail]:
        """Retain finalization after bounded public cancellation."""
        cleanup_task = asyncio.create_task(
            self._finalize_failed_market_add(
                connector_name,
                trading_pair,
                connector,
                connector_preexisting,
                connector_incarnation,
            )
        )
        retained = self._retain_market_transaction_descendant(
            connector_name, cleanup_task
        )
        if retained is False:
            cleanup_task.cancel()
            await asyncio.gather(cleanup_task, return_exceptions=True)
            detail = CapabilityErrorDetail(
                code="market_cleanup_residual",
                message=(
                    f"Cleanup is incomplete for "
                    f"'{connector_name}/{trading_pair}'"
                ),
                connector=connector_name,
                feature=ConnectorFeature.LIVE_ORDER_BOOK.value,
                trading_pair=trading_pair,
                reason="cleanup_incomplete",
                cleanup_scope=(
                    "pair" if connector_preexisting else "connector"
                ),
                cleanup_code="cleanup_authority_unavailable",
            )
            key = self._residual_key(
                connector_name,
                trading_pair,
                "pair" if connector_preexisting else "connector",
            )
            self._cleanup_residuals[key] = detail.model_dump(
                exclude_none=True
            )
            self._cleanup_owners[key] = {
                "connector": connector,
                "connector_preexisting": connector_preexisting,
                "incarnation": connector_incarnation,
            }
            return detail
        return await asyncio.shield(cleanup_task)

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
        try:
            self._require_capability(
                connector_name, ConnectorFeature.DYNAMIC_SUBSCRIPTION, trading_pair
            )
        except ConnectorCapabilityError as exc:
            # Executor cleanup is command-style: return a typed rejection while
            # preserving membership so callers can surface it without ambiguity.
            return CapabilityErrorDetail.model_validate(exc.detail)
        async with self._market_transaction(connector_name):
            if (
                trading_pair not in self._markets.get(connector_name, set())
                or not self._owns_market_pair(connector_name, trading_pair)
            ):
                raise ConnectorCapabilityError(
                    CapabilityErrorDetail(
                        code="market_ownership_required",
                        message=(
                            f"This interface does not own "
                            f"'{connector_name}/{trading_pair}'"
                        ),
                        connector=connector_name,
                        feature=ConnectorFeature.DYNAMIC_SUBSCRIPTION.value,
                        trading_pair=trading_pair,
                        reason="pair ownership is scoped to the creating interface",
                    )
                )

            # Releasing one of several reference tokens is purely local. The final
            # token owns the remote unsubscribe and shared connector/tracker cleanup.
            if self._has_other_market_owners(connector_name, trading_pair):
                self._remove_local_market_ownership(connector_name, trading_pair)
                logger.info(
                    "Released local market ownership for %s/%s; peer ownership remains",
                    connector_name,
                    trading_pair,
                )
                return

            # Remote cleanup must succeed before local membership is committed.
            if remove_order_book:
                result = await self._market_data_service.remove_trading_pair(
                    connector_name=connector_name,
                    trading_pair=trading_pair,
                    account_name=self._account_name
                )
                if result is False:
                    raise ConnectorCapabilityError(
                        CapabilityErrorDetail(
                            code="market_cleanup_residual",
                            message=f"Cleanup is incomplete for '{connector_name}/{trading_pair}'",
                            connector=connector_name,
                            feature=ConnectorFeature.DYNAMIC_SUBSCRIPTION.value,
                            trading_pair=trading_pair,
                            reason="cleanup_incomplete",
                            cleanup_scope="pair",
                            cleanup_code="cleanup_returned_false",
                        )
                    )

            self._remove_local_market_ownership(connector_name, trading_pair)

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
        self._require_capability(
            connector_name, ConnectorFeature.ORDER_SUBMISSION, trading_pair
        )
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
        self._require_capability(
            connector_name, ConnectorFeature.ORDER_SUBMISSION, trading_pair
        )
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
        self._require_capability(
            connector_name, ConnectorFeature.ORDER_SUBMISSION, trading_pair
        )
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
        self._require_capability(connector_name, ConnectorFeature.ORDER_SUBMISSION)
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
        for connector_name, pairs in list(self._markets.items()):
            for trading_pair in list(pairs):
                connector = self.connectors.get(connector_name)
                peer_owned = self._has_other_market_owners(
                    connector_name, trading_pair
                )
                self._remove_local_market_ownership(
                    connector_name, trading_pair
                )
                tracker = getattr(connector, "order_book_tracker", None)
                remote_state_remains = (
                    connector is not None
                    and self._pair_exists_in_connector_state(
                        connector, tracker, trading_pair
                    )
                )
                if peer_owned or not remote_state_remains:
                    continue
                key = self._residual_key(
                    connector_name, trading_pair, "pair"
                )
                detail = CapabilityErrorDetail(
                    code="market_cleanup_residual",
                    message=(
                        f"Cleanup is incomplete for "
                        f"'{connector_name}/{trading_pair}'"
                    ),
                    connector=connector_name,
                    feature=ConnectorFeature.DYNAMIC_SUBSCRIPTION.value,
                    trading_pair=trading_pair,
                    reason="cleanup_incomplete",
                    cleanup_scope="pair",
                    cleanup_code="interface_cleanup_pending",
                )
                self._cleanup_residuals.setdefault(
                    key, detail.model_dump(exclude_none=True)
                )
                self._cleanup_owners.setdefault(
                    key,
                    {
                        "connector": connector,
                        "connector_preexisting": True,
                    },
                )
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
