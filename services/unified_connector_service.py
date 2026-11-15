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
import copy
import inspect
import logging
import time
from contextlib import asynccontextmanager
from contextvars import ContextVar
from decimal import Decimal
from itertools import count
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
_TRACKER_TASK_CONTAINERS = (
    "_tracking_tasks",
    "_generation_tasks",
    "_draining_tasks",
    "_generation_drain_tasks",
    "_tasks_to_drain",
    "_shutdown_unsubscribe_tasks",
    "_in_flight_transactions",
)
_INSTANCE_FEATURE_CALLABLE_CHAINS = MappingProxyType(
    {
        ConnectorFeature.ORDER_SUBMISSION.value: ("buy", "sell", "cancel"),
    }
)
_PUBLIC_TRANSACTION_CONTEXT = ContextVar(
    "public_transaction_context", default=None
)
_MARKET_TRANSACTION_CONTEXT = ContextVar(
    "market_transaction_context", default=()
)
_PUBLIC_TASK_FACTORY_STATES: Dict[Any, Dict[str, Any]] = {}
_PUBLIC_TRANSACTION_TOKEN_IDS = count(1)
_OWNER_AUTHORITY_UNSET = object()


class _PublicTransactionToken:
    """Lightweight context payload whose snapshot exists only in an active service map."""

    __slots__ = ("id", "active")

    def __init__(self, token_id: int):
        self.id = token_id
        self.active = True


class _MarketTransactionLease:
    """Keep one physical market lock alive through all retained descendants."""

    __slots__ = (
        "active",
        "acquired",
        "frames",
        "guardian_task",
        "lock",
        "pruned",
        "release_requested",
        "root_exited",
        "root_task",
    )

    def __init__(self, owner_task: asyncio.Task, lock: asyncio.Lock):
        loop = asyncio.get_running_loop()
        self.active = True
        self.acquired = loop.create_future()
        self.frames: Dict[asyncio.Task, Optional[asyncio.Task]] = {}
        self.guardian_task: Optional[asyncio.Task] = None
        self.lock: Optional[asyncio.Lock] = lock
        self.pruned = False
        self.release_requested = asyncio.Event()
        self.root_exited = False
        self.root_task: Optional[asyncio.Task] = owner_task

    def retain(self, parent_task: asyncio.Task, child_task: asyncio.Task) -> bool:
        """Retain an exact child frame without granting it mutation authority."""
        parent_is_root = (
            not self.root_exited and self.root_task is parent_task
        )
        if (
            not self.active
            or child_task is parent_task
            or child_task.done()
            or child_task in self.frames
            or (not parent_is_root and parent_task not in self.frames)
        ):
            return False
        self.frames[child_task] = parent_task
        child_task.add_done_callback(self._frame_completed)
        return True

    def exit_root(self) -> None:
        self.root_exited = True
        self.root_task = None
        self._request_release_if_terminal()

    def _frame_completed(self, completed_task: asyncio.Task) -> None:
        self.frames.pop(completed_task, None)
        self._request_release_if_terminal()

    def reap_terminal_frames(self) -> None:
        """Synchronously close the terminal handoff before a new caller queues."""
        for task in tuple(self.frames):
            if task.done():
                self.frames.pop(task, None)
        self._request_release_if_terminal()

    def _request_release_if_terminal(self) -> None:
        if self.root_exited and not self.frames:
            self.release_requested.set()

    def close(self) -> None:
        """Revoke stale contexts and sever every task reference exactly once."""
        self.active = False
        self.root_exited = True
        self.root_task = None
        self.frames.clear()
        self.lock = None
        self.guardian_task = None


class _MarketTransactionToken:
    """Context proof for the root task of one retained physical-lock lease."""

    __slots__ = ("lease",)

    def __init__(self, lease: _MarketTransactionLease):
        self.lease = lease

    @property
    def active(self) -> bool:
        return self.lease.active

    @property
    def owner_task(self) -> Optional[asyncio.Task]:
        return self.lease.root_task if self.lease.active else None


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
        self._active_public_transaction_tokens: Dict[int, Dict[str, Any]] = {}
        self._connector_incarnations: Dict[Tuple[str, str], int] = {}
        self._connector_incarnation_sequence = 0
        self._account_transaction_locks: Dict[str, asyncio.Lock] = {}
        self._account_transaction_lock_users: Dict[str, int] = {}
        self._account_transaction_owners: Dict[str, asyncio.Task] = {}
        self._account_transaction_locks_guard = asyncio.Lock()
        self._market_locks: Dict[Tuple[str, str], asyncio.Lock] = {}
        self._market_lock_users: Dict[Tuple[str, str], int] = {}
        self._market_leases: Dict[
            Tuple[str, str], _MarketTransactionLease
        ] = {}
        self._market_state_versions: Dict[Tuple[str, str], int] = {}
        self._market_state_waiters: Dict[Tuple[str, str], asyncio.Future] = {}
        self._market_locks_guard = asyncio.Lock()
        self._market_pair_owners: Dict[Tuple[str, str, str], Set[object]] = {}
        self._market_cleanup_residuals: Dict[Tuple[str, ...], Dict[str, Any]] = {}
        self._market_cleanup_owners: Dict[Tuple[str, ...], Dict[str, Any]] = {}
        self._market_residual_tasks: Dict[Tuple[str, ...], Set[asyncio.Task]] = {}
        self._market_cleanup_phases: Dict[Tuple[str, ...], Dict[str, Any]] = {}

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
        if connector is not None:
            return self._validate_instance_capabilities(connector_name, connector)
        return self._capability_registry.get(connector_name)

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
        capabilities = (
            self._validate_instance_capabilities(connector_name, connector)
            if connector is not None
            else self._capability_registry.get(connector_name)
        )
        self._capability_registry.require_resolved(
            connector_name, capabilities, feature, trading_pair
        )

    def _validate_instance_capabilities(
        self, connector_name: str, connector: ConnectorBase
    ) -> ConnectorCapabilities:
        """Validate instance-only feature chains before publishing the connector."""
        capabilities = self._capability_registry.get(connector_name, connector)
        instance_only_features = self._capability_registry.instance_only_features(
            connector_name, capabilities
        )
        validated_features = set()
        for feature in instance_only_features:
            member_names = _INSTANCE_FEATURE_CALLABLE_CHAINS.get(feature)
            if member_names is None:
                continue
            try:
                chain_is_callable = all(
                    callable(getattr(connector, member_name, None))
                    for member_name in member_names
                )
            except Exception:
                self._capability_registry.raise_mismatch(connector_name)
            if chain_is_callable:
                validated_features.add(feature)
        return self._capability_registry.validate_instance(
            connector_name,
            connector,
            validated_features=frozenset(validated_features),
            capabilities=capabilities,
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
        if not hasattr(self, "_connector_locks"):
            self._connector_locks = {}
        if not hasattr(self, "_connector_lock_users"):
            self._connector_lock_users = {}
        if not hasattr(self, "_connector_locks_guard"):
            self._connector_locks_guard = asyncio.Lock()
        defaults = (
            ("_connector_creation_residuals", {}),
            ("_connector_creation_failures", {}),
            ("_connector_cleanup_owners", {}),
            ("_connector_cleanup_indices", {}),
            ("_connector_residual_tasks", {}),
            ("_connector_cleanup_phases", {}),
            ("_connector_incarnations", {}),
        )
        for name, value in defaults:
            if not hasattr(self, name):
                setattr(self, name, value)

        if not hasattr(self, "_connector_incarnation_sequence"):
            self._connector_incarnation_sequence = 0

        if not hasattr(self, "_account_transaction_locks"):
            self._account_transaction_locks = {}
        if not hasattr(self, "_account_transaction_lock_users"):
            self._account_transaction_lock_users = {}
        if not hasattr(self, "_account_transaction_owners"):
            self._account_transaction_owners = {}
        if not hasattr(self, "_account_transaction_locks_guard"):
            self._account_transaction_locks_guard = asyncio.Lock()

        if not hasattr(self, "_market_locks"):
            self._market_locks = {}
        if not hasattr(self, "_market_lock_users"):
            self._market_lock_users = {}
        if not hasattr(self, "_market_leases"):
            self._market_leases = {}
        if not hasattr(self, "_market_state_versions"):
            self._market_state_versions = {}
        if not hasattr(self, "_market_state_waiters"):
            self._market_state_waiters = {}
        if not hasattr(self, "_market_locks_guard"):
            self._market_locks_guard = asyncio.Lock()
        market_defaults = (
            ("_market_pair_owners", {}),
            ("_market_cleanup_residuals", {}),
            ("_market_cleanup_owners", {}),
            ("_market_residual_tasks", {}),
            ("_market_cleanup_phases", {}),
        )
        for name, value in market_defaults:
            if not hasattr(self, name):
                setattr(self, name, value)

    def _connector_incarnation(
        self, connector_key: Tuple[str, str]
    ) -> Optional[int]:
        self._ensure_connector_cleanup_state()
        return self._connector_incarnations.get(connector_key)

    def _advance_connector_incarnation(
        self, connector_key: Tuple[str, str]
    ) -> int:
        """Publish a globally unique token for one active physical owner."""
        self._ensure_connector_cleanup_state()
        self._connector_incarnation_sequence += 1
        incarnation = self._connector_incarnation_sequence
        self._connector_incarnations[connector_key] = incarnation
        return incarnation

    def _cached_public_connector(
        self, connector_key: Tuple[str, str]
    ) -> Optional[ConnectorBase]:
        """Read the current cache without constructing a data connector."""
        account_name, connector_name = connector_key
        if account_name == "__data__":
            return getattr(self, "_data_connectors", {}).get(connector_name)
        return getattr(self, "_trading_connectors", {}).get(
            account_name, {}
        ).get(connector_name)

    def _connector_has_private_state(
        self, connector_key: Tuple[str, str]
    ) -> bool:
        registries = (
            getattr(self, "_connector_creation_residuals", {}),
            getattr(self, "_connector_creation_failures", {}),
            getattr(self, "_connector_cleanup_indices", {}),
            getattr(self, "_connector_cleanup_owners", {}),
            getattr(self, "_connector_residual_tasks", {}),
            getattr(self, "_connector_cleanup_phases", {}),
            getattr(self, "_market_cleanup_residuals", {}),
            getattr(self, "_market_cleanup_owners", {}),
            getattr(self, "_market_pair_owners", {}),
            getattr(self, "_market_residual_tasks", {}),
            getattr(self, "_market_cleanup_phases", {}),
        )
        return any(
            (
                raw_key == connector_key
                or (
                    isinstance(raw_key, tuple)
                    and len(raw_key) >= 2
                    and raw_key[:2] == connector_key
                )
            )
            for registry in registries
            for raw_key in registry
        )

    def _prune_connector_incarnation(
        self, connector_key: Tuple[str, str]
    ) -> None:
        """Forget terminal owners; uniqueness lives in the global sequence."""
        self._ensure_connector_cleanup_state()
        if self._cached_public_connector(connector_key) is not None:
            return
        if self._connector_lock_users.get(connector_key, 0):
            return
        if self._connector_has_private_state(connector_key):
            return
        self._connector_incarnations.pop(connector_key, None)

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
        exact_lease = self._exact_market_transaction_lease(connector_key)
        if exact_lease is not None:
            owner = self._account_transaction_owners.get(account_name)
            if owner is not None and owner is not asyncio.current_task():
                raise RuntimeError("account_transaction_active")
            return await self._get_trading_connector_already_locked(
                account_name, connector_name
            )
        self._raise_market_mutation_preflight(connector_key)
        async with self.account_transaction(account_name):
            async with self._market_transaction_lock(connector_key):
                return await self._get_trading_connector_already_locked(
                    account_name, connector_name
                )

    async def _get_trading_connector_already_locked(
        self,
        account_name: str,
        connector_name: str,
    ) -> ConnectorBase:
        """Create or return a connector under exact-root market authority."""
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
            if self._connector_has_private_state(connector_key):
                detail = CapabilityErrorDetail(
                    code="connector_cleanup_residual",
                    message=(
                        f"Connector cleanup is incomplete for "
                        f"'{account_name}:{connector_name}'"
                    ),
                    connector=connector_name,
                    feature="connector_cleanup",
                    reason="cleanup_incomplete",
                    cleanup_scope="connector",
                    cleanup_code="owner_cleanup_incomplete",
                )
                residual = detail.model_dump(exclude_none=True)
                self._connector_creation_residuals[connector_key] = residual
                self._signal_market_state_change(connector_key)
                raise ConnectorCapabilityError(detail)

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
            self._advance_connector_incarnation(connector_key)
            self._connector_creation_residuals.pop(connector_key, None)
            return connector

    @staticmethod
    def _tasks_have_live_work(task_registry, connector_key: Tuple[str, str]) -> bool:
        try:
            current_task = asyncio.current_task()
        except RuntimeError:
            current_task = None
        return any(
            task is not current_task and not task.done()
            for owner_key, tasks in task_registry.items()
            if isinstance(owner_key, tuple) and owner_key[:2] == connector_key
            for task in tasks
        )

    def _market_state_change_snapshot(
        self, connector_key: Tuple[str, str]
    ) -> Tuple[int, asyncio.Future]:
        """Return a race-free generation and waiter for same-key state changes."""
        self._ensure_connector_cleanup_state()
        waiter = self._market_state_waiters.get(connector_key)
        if waiter is None or waiter.done():
            waiter = asyncio.get_running_loop().create_future()
            self._market_state_waiters[connector_key] = waiter
        return self._market_state_versions.get(connector_key, 0), waiter

    def _signal_market_state_change(
        self, connector_key: Tuple[str, str]
    ) -> None:
        """Wake queued guardians whenever lease or residual truth changes."""
        self._ensure_connector_cleanup_state()
        self._market_state_versions[connector_key] = (
            self._market_state_versions.get(connector_key, 0) + 1
        )
        waiter = self._market_state_waiters.pop(connector_key, None)
        if waiter is not None and not waiter.done():
            waiter.set_result(None)

    def _market_mutation_preflight_detail(
        self,
        connector_key: Tuple[str, str],
        trading_pair: Optional[str] = None,
        *,
        current_lease: Optional[_MarketTransactionLease] = None,
        allow_active_lease_wait: bool = False,
    ) -> Optional[CapabilityErrorDetail]:
        """Return same-key live cleanup truth before another mutator can queue."""
        self._ensure_connector_cleanup_state()
        lease = self._market_leases.get(connector_key)
        if lease is not None and lease.active:
            lease.reap_terminal_frames()
        foreign_lease = bool(
            lease is not None
            and lease is not current_lease
            and lease.active
        )
        live_tasks = self._tasks_have_live_work(
            self._connector_residual_tasks, connector_key
        ) or self._tasks_have_live_work(
            self._market_residual_tasks, connector_key
        )
        if not foreign_lease and not live_tasks:
            return None

        market_residuals = self._market_cleanup_residuals
        preferred_keys = []
        if trading_pair is not None:
            preferred_keys.append((*connector_key, "pair", trading_pair))
        preferred_keys.append((*connector_key, "connector"))
        for residual_key in preferred_keys:
            residual = market_residuals.get(residual_key)
            if residual is not None:
                return CapabilityErrorDetail.model_validate(residual)
        for residual_key, residual in market_residuals.items():
            if (
                isinstance(residual_key, tuple)
                and residual_key[:2] == connector_key
            ):
                return CapabilityErrorDetail.model_validate(residual)

        residual = self._connector_creation_residuals.get(connector_key)
        if residual is not None:
            return CapabilityErrorDetail.model_validate(residual)

        if foreign_lease and allow_active_lease_wait and not live_tasks:
            return None

        account_name, connector_name = connector_key
        live_owner_kinds = {
            owner_key[2]
            for owner_key, tasks in self._connector_residual_tasks.items()
            if (
                isinstance(owner_key, tuple)
                and owner_key[:2] == connector_key
                and any(not task.done() for task in tasks)
            )
        }
        cleanup_code = (
            "restart_cleanup_incomplete"
            if "restart" in live_owner_kinds
            else "market_transaction_retained"
        )
        return CapabilityErrorDetail(
            code="connector_cleanup_residual",
            message=(
                f"Connector cleanup is incomplete for "
                f"'{account_name}:{connector_name}'"
            ),
            connector=connector_name,
            feature="connector_cleanup",
            reason="cleanup_incomplete",
            cleanup_scope="connector",
            cleanup_code=cleanup_code,
        )

    def _raise_market_mutation_preflight(
        self,
        connector_key: Tuple[str, str],
        trading_pair: Optional[str] = None,
        *,
        current_lease: Optional[_MarketTransactionLease] = None,
        allow_active_lease_wait: bool = False,
    ) -> None:
        detail = self._market_mutation_preflight_detail(
            connector_key,
            trading_pair,
            current_lease=current_lease,
            allow_active_lease_wait=allow_active_lease_wait,
        )
        if detail is not None:
            raise ConnectorCapabilityError(detail)

    @asynccontextmanager
    async def _market_transaction_lock(
        self,
        connector_key: Tuple[str, str],
        trading_pair: Optional[str] = None,
    ):
        """Serialize mutation while a guardian retains live physical descendants."""
        self._ensure_connector_cleanup_state()
        scope = (id(self), connector_key)
        inherited = _MARKET_TRANSACTION_CONTEXT.get()
        current_task = asyncio.current_task()
        if current_task is None:
            raise RuntimeError("market transaction lock requires an asyncio.Task")
        for inherited_scope, inherited_token in reversed(inherited):
            if inherited_scope != scope or not inherited_token.active:
                continue
            lease = inherited_token.lease
            if lease.root_task is current_task and not lease.frames:
                yield inherited_token
                return
            raise RuntimeError("market_transaction_authority_unavailable")

        self._raise_market_mutation_preflight(connector_key, trading_pair)
        async with self._market_locks_guard:
            lock = self._market_locks.setdefault(connector_key, asyncio.Lock())
            self._market_lock_users[connector_key] = (
                self._market_lock_users.get(connector_key, 0) + 1
            )
        lease = _MarketTransactionLease(current_task, lock)
        guardian_task = asyncio.create_task(
            self._guard_market_transaction_lock(connector_key, lock, lease)
        )
        lease.guardian_task = guardian_task
        transaction_token = None
        context_token = None
        try:
            await asyncio.shield(lease.acquired)
            transaction_token = _MarketTransactionToken(lease)
            context_token = _MARKET_TRANSACTION_CONTEXT.set(
                (*inherited, (scope, transaction_token))
            )
            self._raise_market_mutation_preflight(
                connector_key,
                trading_pair,
                current_lease=lease,
            )
            yield transaction_token
        finally:
            if context_token is not None:
                _MARKET_TRANSACTION_CONTEXT.reset(context_token)
            lease.exit_root()
            if transaction_token is None:
                guardian_task.cancel()
                await self._await_market_guardian(guardian_task)
            elif not lease.frames:
                await self._await_market_guardian(guardian_task)

    @staticmethod
    async def _await_market_guardian(guardian_task: asyncio.Task) -> None:
        """Reap a guardian without forwarding repeated caller cancellation."""
        cancelled = False
        while not guardian_task.done():
            try:
                await asyncio.shield(guardian_task)
            except asyncio.CancelledError:
                cancelled = True
                continue
        try:
            guardian_task.result()
        except asyncio.CancelledError:
            pass
        if cancelled:
            raise asyncio.CancelledError()

    def _finish_market_guardian(
        self,
        connector_key: Tuple[str, str],
        lock: asyncio.Lock,
        lease: _MarketTransactionLease,
        acquired: bool,
    ) -> None:
        """Synchronously release and prune exactly once, even under cancellation."""
        if lease.pruned:
            return
        lease.pruned = True
        lease.close()
        if acquired and lock.locked():
            lock.release()
        if self._market_leases.get(connector_key) is lease:
            self._market_leases.pop(connector_key, None)
        users = max(self._market_lock_users.get(connector_key, 1) - 1, 0)
        if users:
            self._market_lock_users[connector_key] = users
            self._signal_market_state_change(connector_key)
        else:
            self._market_lock_users.pop(connector_key, None)
            if self._market_locks.get(connector_key) is lock:
                self._market_locks.pop(connector_key, None)
            waiter = self._market_state_waiters.pop(connector_key, None)
            if waiter is not None and not waiter.done():
                waiter.set_result(None)
            self._market_state_versions.pop(connector_key, None)

    async def _guard_market_transaction_lock(
        self,
        connector_key: Tuple[str, str],
        lock: asyncio.Lock,
        lease: _MarketTransactionLease,
    ) -> None:
        """Own acquisition, terminal release, and registry pruning for a lease."""
        acquired = False
        acquire_task = asyncio.create_task(lock.acquire())
        try:
            while not acquire_task.done():
                version, state_changed = self._market_state_change_snapshot(
                    connector_key
                )
                detail = self._market_mutation_preflight_detail(
                    connector_key, allow_active_lease_wait=True
                )
                if detail is not None:
                    acquire_task.cancel()
                    await asyncio.gather(acquire_task, return_exceptions=True)
                    if not lease.acquired.done():
                        lease.acquired.set_exception(
                            ConnectorCapabilityError(detail)
                        )
                    return
                if self._market_state_versions.get(connector_key, 0) != version:
                    continue
                await asyncio.wait(
                    {acquire_task, state_changed},
                    return_when=asyncio.FIRST_COMPLETED,
                )
            acquire_task.result()
            acquired = True
            async with self._market_locks_guard:
                self._market_leases[connector_key] = lease
            self._signal_market_state_change(connector_key)
            if not lease.acquired.done():
                lease.acquired.set_result(None)
            await lease.release_requested.wait()
        except asyncio.CancelledError:
            if not lease.acquired.done():
                lease.acquired.cancel()
            raise
        except BaseException as exc:
            if not lease.acquired.done():
                lease.acquired.set_exception(exc)
            raise
        finally:
            if not acquire_task.done():
                acquire_task.cancel()
                acquire_task.add_done_callback(self._consume_background_task_result)
            self._finish_market_guardian(
                connector_key, lock, lease, acquired
            )

    def _retain_market_transaction_descendant(
        self,
        connector_key: Tuple[str, str],
        descendant_task: asyncio.Task,
    ) -> bool:
        """Retain a physical descendant without allowing it to reenter the lock."""
        if not isinstance(descendant_task, asyncio.Task):
            raise TypeError("market transaction retention requires an asyncio.Task")
        current_task = asyncio.current_task()
        if current_task is None:
            return False
        scope = (id(self), connector_key)
        inherited = _MARKET_TRANSACTION_CONTEXT.get()
        for inherited_scope, transaction_token in reversed(inherited):
            if inherited_scope != scope or not transaction_token.active:
                continue
            return transaction_token.lease.retain(
                current_task, descendant_task
            )
        return False

    def _active_market_transaction_lease(
        self, connector_key: Tuple[str, str]
    ) -> Optional[_MarketTransactionLease]:
        scope = (id(self), connector_key)
        for inherited_scope, transaction_token in reversed(
            _MARKET_TRANSACTION_CONTEXT.get()
        ):
            if inherited_scope == scope and transaction_token.active:
                return transaction_token.lease
        return None

    def _is_market_transaction_participant(
        self, connector_key: Tuple[str, str]
    ) -> bool:
        lease = self._active_market_transaction_lease(connector_key)
        try:
            current_task = asyncio.current_task()
        except RuntimeError:
            current_task = None
        if lease is None or current_task is None:
            return False
        lease.reap_terminal_frames()
        return lease.root_task is current_task and not lease.frames

    def _exact_market_transaction_lease(
        self, connector_key: Tuple[str, str]
    ) -> Optional[_MarketTransactionLease]:
        """Return authority only to the exact active root task."""
        lease = self._active_market_transaction_lease(connector_key)
        return lease if self._is_market_transaction_participant(connector_key) else None

    def _market_transaction_has_live_descendant(
        self, connector_key: Tuple[str, str]
    ) -> bool:
        """Report physical frames other than the exact task asking."""
        lease = self._active_market_transaction_lease(connector_key)
        current_task = asyncio.current_task()
        return bool(
            lease is not None
            and any(
                task is not current_task and not task.done()
                for task in lease.frames
            )
        )

    @asynccontextmanager
    async def _connector_creation_lock(self, connector_key: Tuple[str, str]):
        """Serialize creation and prune the caller-controlled cache key after use."""
        self._ensure_connector_cleanup_state()
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
                    if self._market_lock_users.get(connector_key, 0) <= 1:
                        getattr(self, "_connector_creation_failures", {}).pop(
                            connector_key, None
                        )
                    self._prune_connector_incarnation(connector_key)

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
            self._ensure_connector_cleanup_state()
            connector_key = self._connector_key("__data__", connector_name)
            lease = self._exact_market_transaction_lease(connector_key)
            self._raise_market_mutation_preflight(
                connector_key, current_lease=lease
            )
            inherited_lease = self._active_market_transaction_lease(connector_key)
            if inherited_lease is not None and lease is None:
                raise RuntimeError("market_transaction_authority_unavailable")
            residual = self._connector_creation_residuals.get(connector_key)
            if residual is None and self._connector_has_private_state(connector_key):
                detail = CapabilityErrorDetail(
                    code="connector_cleanup_residual",
                    message=(
                        f"Connector cleanup is incomplete for "
                        f"'__data__:{connector_name}'"
                    ),
                    connector=connector_name,
                    feature="connector_cleanup",
                    reason="cleanup_incomplete",
                    cleanup_scope="connector",
                    cleanup_code="owner_cleanup_incomplete",
                )
                residual = detail.model_dump(exclude_none=True)
                self._connector_creation_residuals[connector_key] = residual
                self._signal_market_state_change(connector_key)
            if residual is not None:
                raise ConnectorCapabilityError(
                    CapabilityErrorDetail.model_validate(residual)
                )
            connector = self._create_data_connector(connector_name)
            self._validate_instance_capabilities(connector_name, connector)
            self._data_connectors[connector_name] = connector
            self._advance_connector_incarnation(
                self._connector_key("__data__", connector_name)
            )
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
        connector_key = self._connector_key("__data__", connector_name)
        async with self._market_transaction_lock(connector_key, trading_pair):
            return await self._ensure_data_connector_started_already_locked(
                connector_name, trading_pair, connector_key
            )

    async def _ensure_data_connector_started_already_locked(
        self,
        connector_name: str,
        trading_pair: str,
        connector_key: Tuple[str, str],
    ) -> bool:
        """Start a data connector under exact-root market authority."""
        async with self._connector_creation_lock(connector_key):
            await self._retry_public_transaction_cleanup(connector_key)
            residual = self._connector_creation_residuals.get(connector_key)
            if residual is not None:
                raise ConnectorCapabilityError(
                    CapabilityErrorDetail.model_validate(residual)
                )
            initial_cache_state = self._capture_public_cache_state(connector_key)
            connector = self.get_data_connector(connector_name)
            snapshot = self._snapshot_public_connector_state(
                connector_name,
                connector,
                connector_key,
                self._capability_registry.get(connector_name),
                initial_cache_state=initial_cache_state,
                trading_pair=trading_pair,
            )
            try:
                approved_capabilities = self._validate_instance_capabilities(
                    connector_name, connector
                )
            except ConnectorCapabilityError:
                await self._discard_invalid_public_declaration(
                    connector_name, connector, snapshot
                )
                raise
            snapshot["approved_capabilities"] = approved_capabilities
            return await self._run_public_initialization_boundary(
                snapshot,
                "start",
                self._ensure_data_connector_started_locked(
                    connector_name, trading_pair, connector, snapshot
                ),
            )

    async def _ensure_data_connector_started_locked(
        self,
        connector_name: str,
        trading_pair: str,
        connector: ConnectorBase,
        snapshot: Dict[str, Any],
    ) -> bool:
        """Start one owned data incarnation and validate its final declaration."""
        self._require_public_connector_ownership(snapshot)
        if self._data_connectors_started.get(connector_name, False):
            return True

        if trading_pair not in connector._trading_pairs:
            connector._trading_pairs.append(trading_pair)
            self._record_public_pair_deltas(snapshot)

        start_result = await connector.start_network()
        if start_result is False:
            return False
        self._require_public_connector_ownership(snapshot)

        max_wait = 30
        waited = 0
        tracker = connector.order_book_tracker
        while waited < max_wait:
            if tracker._order_book_stream_listener_task is not None:
                await asyncio.sleep(2.0)
                self._require_public_connector_ownership(snapshot)
                break
            await asyncio.sleep(0.5)
            self._require_public_connector_ownership(snapshot)
            waited += 0.5

        final_capabilities = self._validate_instance_capabilities(
            connector_name, connector
        )
        self._capability_registry.require_resolved(
            connector_name,
            final_capabilities,
            ConnectorFeature.DYNAMIC_SUBSCRIPTION,
            trading_pair,
        )
        self._capability_registry.require_resolved(
            connector_name,
            final_capabilities,
            ConnectorFeature.LIVE_ORDER_BOOK,
            trading_pair,
        )
        self._require_public_connector_ownership(snapshot)
        started = self._data_connectors_started
        snapshot["owned_started_delta"] = {
            "baseline_present": connector_name in started,
            "baseline_value": started.get(connector_name),
            "owned_value": True,
        }
        started[connector_name] = True
        logger.info(
            "Started data connector: %s with pair %s",
            connector_name,
            trading_pair,
        )
        return True

    def _resolve_public_connector_key(
        self, connector_name: str, account_name: Optional[str]
    ) -> Tuple[str, str]:
        if account_name is not None:
            return self._connector_key(account_name, connector_name)
        for owner_account, connectors in getattr(
            self, "_trading_connectors", {}
        ).items():
            if connector_name in connectors:
                return self._connector_key(owner_account, connector_name)
        return self._connector_key("__data__", connector_name)

    def _connector_for_public_key(
        self, connector_key: Tuple[str, str]
    ) -> Optional[ConnectorBase]:
        account_name, connector_name = connector_key
        if account_name == "__data__":
            if not hasattr(self, "_data_connectors"):
                return self.get_best_connector_for_market(connector_name)
            return self.get_data_connector(connector_name)
        return getattr(self, "_trading_connectors", {}).get(
            account_name, {}
        ).get(connector_name)

    def _capture_public_cache_state(
        self, connector_key: Tuple[str, str]
    ) -> Dict[str, Any]:
        """Capture the cache/token/start state before first-touch construction."""
        self._ensure_connector_cleanup_state()
        connector = self._cached_public_connector(connector_key)
        incarnation = self._connector_incarnation(connector_key)
        if connector is not None and incarnation is None:
            incarnation = self._advance_connector_incarnation(connector_key)
        account_name, connector_name = connector_key
        started = getattr(self, "_data_connectors_started", {})
        return {
            "cache_present": connector is not None,
            "cache_connector": connector,
            "incarnation": incarnation,
            "data_started_present": (
                account_name == "__data__" and connector_name in started
            ),
            "data_started": (
                started.get(connector_name) if account_name == "__data__" else None
            ),
        }

    @staticmethod
    def _snapshot_container(owner, attribute: str) -> Optional[Dict[str, Any]]:
        try:
            value = getattr(owner, attribute)
        except BaseException:
            return None
        if isinstance(value, dict):
            contents = dict(value)
            kind = "dict"
        elif isinstance(value, list):
            contents = list(value)
            kind = "list"
        elif isinstance(value, set):
            contents = set(value)
            kind = "set"
        else:
            return None
        return {
            "owner": owner,
            "attribute": attribute,
            "value": value,
            "contents": contents,
            "kind": kind,
        }

    @staticmethod
    def _snapshot_task_attributes(owner) -> Dict[str, Tuple[bool, Any]]:
        attributes = {}
        for name, value in getattr(owner, "__dict__", {}).items():
            if "task" in name:
                attributes[name] = (True, value)
        return attributes

    def _snapshot_public_connector_state(
        self,
        connector_name: str,
        connector: ConnectorBase,
        connector_key: Tuple[str, str],
        approved_capabilities: ConnectorCapabilities,
        *,
        initial_cache_state: Optional[Mapping[str, Any]] = None,
        trading_pair: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Capture all public mutable state before the first collaborator await."""
        if initial_cache_state is None:
            initial_cache_state = self._capture_public_cache_state(connector_key)
        if self._connector_incarnation(connector_key) is None:
            self._advance_connector_incarnation(connector_key)
        tracker = getattr(connector, "order_book_tracker", None)
        containers = []
        container_targets = [
            (connector, "_trading_pairs"),
            (tracker, "_trading_pairs"),
            (tracker, "order_books"),
            *((tracker, attribute) for attribute in _TRACKER_TASK_CONTAINERS),
        ]
        for owner, attribute in container_targets:
            if owner is None:
                continue
            state = self._snapshot_container(owner, attribute)
            if state is not None:
                containers.append(state)
        try:
            raw_capabilities = getattr(connector, "api_capabilities", None)
            raw_capabilities_contents = copy.deepcopy(raw_capabilities)
            raw_capabilities_copied = True
        except BaseException:
            raw_capabilities = None
            raw_capabilities_contents = None
            raw_capabilities_copied = False
        tasks = self._collect_connector_tasks(connector)
        account_name, cache_connector_name = connector_key
        if account_name == "__data__":
            cache = getattr(self, "_data_connectors", {})
        else:
            cache = getattr(self, "_trading_connectors", {}).get(
                account_name, {}
            )
        owned_cache_delta = None
        if (
            not initial_cache_state["cache_present"]
            and cache.get(cache_connector_name) is connector
        ):
            owned_cache_delta = {
                "container": cache,
                "key": cache_connector_name,
                "owned_value": connector,
            }
        snapshot = {
            "connector_name": connector_name,
            "connector_key": connector_key,
            "connector": connector,
            "incarnation": self._connector_incarnation(connector_key),
            "approved_capabilities": approved_capabilities,
            "raw_capabilities": raw_capabilities,
            "raw_capabilities_contents": raw_capabilities_contents,
            "raw_capabilities_copied": raw_capabilities_copied,
            "initial_cache_state": dict(initial_cache_state),
            "trading_pair": trading_pair,
            "tracker": tracker,
            "containers": containers,
            "connector_task_attributes": self._snapshot_task_attributes(connector),
            "tracker_task_attributes": (
                self._snapshot_task_attributes(tracker)
                if tracker is not None
                else {}
            ),
            "tasks": tasks,
            "task_states": {
                task: (task.done(), task.cancelled(), task.cancelling())
                for task in tasks
            },
            "owned_pair_deltas": [],
            "owned_book_deltas": [],
            "owned_task_deltas": [],
            "owned_tasks": set(),
            "claimed_task_slots": set(),
            "task_slot_ledgers": {},
            "provenance_tasks": set(),
            "owned_started_delta": None,
            "owned_cache_delta": owned_cache_delta,
            "unsafe_mutation": False,
            "request_task": asyncio.current_task(),
            "data_started_present": connector_name
            in getattr(self, "_data_connectors_started", {}),
            "data_started": getattr(self, "_data_connectors_started", {}).get(
                connector_name
            ),
        }
        self._seed_public_task_ledgers(snapshot)
        return snapshot

    @staticmethod
    def _snapshot_state_for(
        snapshot: Mapping[str, Any], owner, attribute: str
    ) -> Optional[Mapping[str, Any]]:
        return next(
            (
                state
                for state in snapshot["containers"]
                if state["owner"] is owner and state["attribute"] == attribute
            ),
            None,
        )

    @staticmethod
    def _task_slot_key(owner, attribute: str, key: Any = None) -> tuple:
        return id(owner), attribute, key

    def _seed_public_task_ledgers(self, snapshot: Dict[str, Any]) -> None:
        """Seed task-slot generations from the transaction baseline."""
        ledgers = snapshot["task_slot_ledgers"]
        for owner, attributes in (
            (snapshot["connector"], snapshot["connector_task_attributes"]),
            (snapshot["tracker"], snapshot["tracker_task_attributes"]),
        ):
            if owner is None:
                continue
            for attribute, (present, value) in attributes.items():
                ledgers[self._task_slot_key(owner, attribute)] = {
                    "owner": owner,
                    "attribute": attribute,
                    "key": None,
                    "baseline_present": present,
                    "baseline_value": value,
                    "generations": [value] if isinstance(value, asyncio.Task) else [],
                }
        tracker = snapshot["tracker"]
        trading_pair = snapshot["trading_pair"]
        if tracker is None:
            return
        for attribute in _TRACKER_TASK_CONTAINERS:
            state = self._snapshot_state_for(snapshot, tracker, attribute)
            if state is None or state["kind"] != "dict":
                continue
            present = trading_pair in state["contents"]
            value = state["contents"].get(trading_pair)
            ledgers[self._task_slot_key(tracker, attribute, trading_pair)] = {
                "owner": tracker,
                "attribute": attribute,
                "key": trading_pair,
                "baseline_present": present,
                "baseline_value": value,
                "generations": [value] if isinstance(value, asyncio.Task) else [],
            }

    def _record_public_task_slot(
        self,
        snapshot: Dict[str, Any],
        owner,
        attribute: str,
        task: asyncio.Task,
        *,
        key: Any = None,
    ) -> None:
        slot_key = self._task_slot_key(owner, attribute, key)
        try:
            value = getattr(owner, attribute)
        except BaseException:
            return
        ledger = snapshot["task_slot_ledgers"].get(slot_key)
        if ledger is None:
            if key is None:
                attributes = (
                    snapshot["connector_task_attributes"]
                    if owner is snapshot["connector"]
                    else snapshot["tracker_task_attributes"]
                )
                baseline_present, baseline_value = attributes.get(
                    attribute, (False, None)
                )
            else:
                baseline = self._snapshot_state_for(snapshot, owner, attribute)
                baseline_present = bool(
                    baseline is not None and key in baseline["contents"]
                )
                baseline_value = (
                    baseline["contents"].get(key) if baseline_present else None
                )
            ledger = {
                "owner": owner,
                "attribute": attribute,
                "key": key,
                "baseline_present": baseline_present,
                "baseline_value": baseline_value,
                "generations": [],
            }
            snapshot["task_slot_ledgers"][slot_key] = ledger
        if key is not None and (
            not isinstance(value, dict) or value.get(key) is not task
        ):
            return
        generations = ledger["generations"]
        if not generations or generations[-1] is not task:
            generations.append(task)
        if task is snapshot["request_task"] or task in snapshot["tasks"]:
            return
        if task not in snapshot["provenance_tasks"]:
            snapshot["unsafe_mutation"] = True
            return
        delta_key = (*slot_key, id(task))
        if delta_key in snapshot["claimed_task_slots"]:
            return
        snapshot["claimed_task_slots"].add(delta_key)
        snapshot["owned_tasks"].add(task)
        snapshot["owned_task_deltas"].append(
            {
                "kind": "attribute" if key is None else "slot",
                "owner": owner,
                "attribute": attribute,
                "key": key,
                "baseline_present": ledger["baseline_present"],
                "baseline_value": ledger["baseline_value"],
                "task": task,
            }
        )

    def _record_public_task_member(
        self,
        snapshot: Dict[str, Any],
        owner,
        attribute: str,
        container,
        task: asyncio.Task,
    ) -> None:
        if task is snapshot["request_task"] or task in snapshot["tasks"]:
            return
        if task not in snapshot["provenance_tasks"]:
            snapshot["unsafe_mutation"] = True
            return
        slot_key = self._task_slot_key(owner, attribute, id(task))
        if slot_key in snapshot["claimed_task_slots"]:
            return
        try:
            if getattr(owner, attribute) is not container:
                return
        except BaseException:
            return
        snapshot["claimed_task_slots"].add(slot_key)
        snapshot["owned_tasks"].add(task)
        snapshot["owned_task_deltas"].append(
            {
                "kind": "member",
                "owner": owner,
                "attribute": attribute,
                "container": container,
                "task": task,
            }
        )

    def _discover_public_transaction_tasks(
        self, snapshot: Dict[str, Any], boundary: str
    ) -> None:
        snapshot["owned_tasks"].update(snapshot["provenance_tasks"])
        tracker = snapshot["tracker"]
        trading_pair = snapshot["trading_pair"]
        if boundary == "start":
            for owner in (snapshot["connector"], tracker):
                if owner is None:
                    continue
                for attribute, value in getattr(owner, "__dict__", {}).items():
                    if "task" in attribute and isinstance(value, asyncio.Task):
                        self._record_public_task_slot(
                            snapshot, owner, attribute, value
                        )
                for ledger in snapshot["task_slot_ledgers"].values():
                    if ledger["owner"] is not owner or ledger["key"] is not None:
                        continue
                    attribute = ledger["attribute"]
                    present = hasattr(owner, attribute)
                    current = getattr(owner, attribute, None)
                    if isinstance(current, asyncio.Task):
                        continue
                    if present != ledger["baseline_present"] or (
                        present and current is not ledger["baseline_value"]
                    ):
                        snapshot["unsafe_mutation"] = True
        if tracker is None:
            return
        for attribute in _TRACKER_TASK_CONTAINERS:
            try:
                value = getattr(tracker, attribute)
            except BaseException:
                continue
            if not isinstance(value, dict):
                if isinstance(value, (list, set)):
                    state = self._snapshot_state_for(
                        snapshot, tracker, attribute
                    )
                    if state is None or value is not state["value"]:
                        snapshot["unsafe_mutation"] = True
                        continue
                    baseline_tasks = {
                        task
                        for task in state["contents"]
                        if isinstance(task, asyncio.Task)
                    }
                    current_tasks = {
                        task for task in value if isinstance(task, asyncio.Task)
                    }
                    if baseline_tasks - current_tasks:
                        snapshot["unsafe_mutation"] = True
                    for task in current_tasks - baseline_tasks:
                        if isinstance(task, asyncio.Task):
                            self._record_public_task_member(
                                snapshot,
                                tracker,
                                attribute,
                                value,
                                task,
                            )
                continue
            task = value.get(trading_pair)
            if isinstance(task, asyncio.Task):
                self._record_public_task_slot(
                    snapshot,
                    tracker,
                    attribute,
                    task,
                    key=trading_pair,
                )
            else:
                ledger = snapshot["task_slot_ledgers"].get(
                    self._task_slot_key(tracker, attribute, trading_pair)
                )
                current_present = trading_pair in value
                current_value = value.get(trading_pair)
                if ledger is not None and (
                    current_present != ledger["baseline_present"]
                    or (
                        current_present
                        and current_value is not ledger["baseline_value"]
                    )
                ):
                    snapshot["unsafe_mutation"] = True

    @asynccontextmanager
    async def _capture_public_task_provenance(
        self, snapshot: Dict[str, Any]
    ):
        """Tag tasks created by the awaited connector operation without polling."""
        loop = asyncio.get_running_loop()
        active_tokens = getattr(self, "_active_public_transaction_tokens", None)
        if active_tokens is None:
            active_tokens = {}
            self._active_public_transaction_tokens = active_tokens
        state = _PUBLIC_TASK_FACTORY_STATES.get(loop)
        if state is None:
            previous = loop.get_task_factory()
            state = {
                "previous": previous,
                "factory": None,
                "users": 0,
                "active_maps": {},
                "active_map_users": {},
            }

            def task_factory(factory_loop, coroutine, **kwargs):
                context = kwargs.get("context")
                transaction_token = (
                    context.get(_PUBLIC_TRANSACTION_CONTEXT)
                    if context is not None
                    else _PUBLIC_TRANSACTION_CONTEXT.get()
                )
                transaction = None
                if (
                    isinstance(transaction_token, _PublicTransactionToken)
                    and transaction_token.active
                ):
                    for active_map in tuple(state["active_maps"].values()):
                        transaction = active_map.get(transaction_token.id)
                        if transaction is not None:
                            break
                if previous is None:
                    task = asyncio.Task(
                        coroutine, loop=factory_loop, **kwargs
                    )
                else:
                    task = previous(factory_loop, coroutine, **kwargs)
                if (
                    isinstance(transaction_token, _PublicTransactionToken)
                    and transaction_token.active
                    and transaction is not None
                    and isinstance(task, asyncio.Task)
                ):
                    transaction["provenance_tasks"].add(task)
                    transaction["owned_tasks"].add(task)
                return task

            state["factory"] = task_factory
            _PUBLIC_TASK_FACTORY_STATES[loop] = state
            loop.set_task_factory(task_factory)
        service_id = id(self)
        state["active_maps"][service_id] = active_tokens
        state["active_map_users"][service_id] = (
            state["active_map_users"].get(service_id, 0) + 1
        )
        state["users"] += 1
        transaction_token = _PublicTransactionToken(
            next(_PUBLIC_TRANSACTION_TOKEN_IDS)
        )
        active_tokens[transaction_token.id] = snapshot
        context_token = _PUBLIC_TRANSACTION_CONTEXT.set(transaction_token)
        try:
            yield
        finally:
            transaction_token.active = False
            active_tokens.pop(transaction_token.id, None)
            try:
                _PUBLIC_TRANSACTION_CONTEXT.reset(context_token)
            finally:
                state["users"] -= 1
                state["active_map_users"][service_id] -= 1
                if state["active_map_users"][service_id] == 0:
                    state["active_map_users"].pop(service_id, None)
                    state["active_maps"].pop(service_id, None)
                if state["users"] == 0:
                    if loop.get_task_factory() is state["factory"]:
                        loop.set_task_factory(state["previous"])
                    _PUBLIC_TASK_FACTORY_STATES.pop(loop, None)

    def _record_public_pair_deltas(self, snapshot: Dict[str, Any]) -> None:
        trading_pair = snapshot["trading_pair"]
        tracker = snapshot["tracker"]
        for owner, attribute in (
            (snapshot["connector"], "_trading_pairs"),
            (tracker, "_trading_pairs"),
        ):
            if owner is None:
                continue
            state = self._snapshot_state_for(snapshot, owner, attribute)
            if state is None or state["kind"] != "list":
                continue
            value = state["value"]
            baseline_count = state["contents"].count(trading_pair)
            current_count = value.count(trading_pair)
            existing = next(
                (
                    delta
                    for delta in snapshot["owned_pair_deltas"]
                    if delta["container"] is value
                ),
                None,
            )
            if existing is not None:
                if current_count != existing["owned_count"]:
                    snapshot["unsafe_mutation"] = True
                continue
            if current_count == baseline_count:
                continue
            if current_count == baseline_count + 1:
                action = "add"
            elif current_count == baseline_count - 1:
                action = "remove"
            else:
                snapshot["unsafe_mutation"] = True
                continue
            if max(baseline_count, current_count) > 1:
                snapshot["unsafe_mutation"] = True
                continue
            snapshot["owned_pair_deltas"].append(
                {
                    "action": action,
                    "owner": owner,
                    "attribute": attribute,
                    "container": value,
                    "value": trading_pair,
                    "baseline_count": baseline_count,
                    "owned_count": current_count,
                }
            )

        if tracker is None:
            return
        state = self._snapshot_state_for(snapshot, tracker, "order_books")
        if state is None or state["kind"] != "dict":
            return
        books = state["value"]
        baseline_present = trading_pair in state["contents"]
        baseline_value = state["contents"].get(trading_pair)
        current_present = trading_pair in books
        existing = next(
            (
                delta
                for delta in snapshot["owned_book_deltas"]
                if delta["container"] is books
                and delta["key"] == trading_pair
            ),
            None,
        )
        if existing is not None:
            expected_present = existing["action"] == "set"
            if current_present != expected_present or (
                current_present
                and books.get(trading_pair) is not existing["owned_value"]
            ):
                snapshot["unsafe_mutation"] = True
            return
        if not current_present and not baseline_present:
            return
        if current_present:
            owned_value = books[trading_pair]
            if baseline_present and owned_value is baseline_value:
                return
            action = "set"
        else:
            owned_value = None
            action = "remove"
        snapshot["owned_book_deltas"].append(
            {
                "action": action,
                "owner": tracker,
                "attribute": "order_books",
                "container": books,
                "key": trading_pair,
                "baseline_present": baseline_present,
                "baseline_value": baseline_value,
                "owned_value": owned_value,
            }
        )

    @staticmethod
    def _capture_public_declaration(connector) -> Tuple[Any, Any, bool]:
        try:
            declaration = getattr(connector, "api_capabilities")
            return declaration, copy.deepcopy(declaration), True
        except BaseException:
            return None, None, False

    def _record_public_declaration_delta(
        self,
        snapshot: Dict[str, Any],
        before: Tuple[Any, Any, bool],
    ) -> None:
        before_value, before_contents, before_copied = before
        after_value, after_contents, after_copied = self._capture_public_declaration(
            snapshot["connector"]
        )
        if not before_copied or not after_copied:
            snapshot["unsafe_mutation"] = True
            return
        if before_value is after_value and before_contents == after_contents:
            return
        # Collaborators own declaration changes made while their coroutine is
        # running. Without an explicit service mutation site there is no safe
        # inverse operation, so retain the residual state and unpublish instead.
        snapshot["unsafe_mutation"] = True

    def _capture_public_boundary_containers(
        self, snapshot: Mapping[str, Any]
    ) -> list:
        captured = []
        for state in snapshot["containers"]:
            current = self._snapshot_container(state["owner"], state["attribute"])
            captured.append(current)
        return captured

    def _public_boundary_containers_changed(self, captured: list) -> bool:
        return any(
            state is None or not self._container_matches_snapshot(state)
            for state in captured
        )

    @staticmethod
    def _public_boundary_container_identity_changed(captured: list) -> bool:
        for state in captured:
            if state is None:
                return True
            try:
                if getattr(state["owner"], state["attribute"]) is not state["value"]:
                    return True
            except BaseException:
                return True
        return False

    async def _run_public_operation_with_ledger(
        self,
        snapshot: Dict[str, Any],
        boundary: str,
        operation,
    ):
        before_declaration = self._capture_public_declaration(snapshot["connector"])
        hidden_boundary = boundary in {"discovery", "book_wait"}
        before_containers = self._capture_public_boundary_containers(snapshot)
        before_tasks = (
            self._collect_connector_tasks(snapshot["connector"])
            if hidden_boundary
            else set()
        )
        if boundary in {"start", "pair"}:
            self._record_public_pair_deltas(snapshot)
            self._discover_public_transaction_tasks(snapshot, boundary)
        try:
            if boundary in {"start", "pair"}:
                async with self._capture_public_task_provenance(snapshot):
                    return await operation
            return await operation
        finally:
            if boundary in {"start", "pair"}:
                self._record_public_pair_deltas(snapshot)
                self._discover_public_transaction_tasks(snapshot, boundary)
            self._record_public_declaration_delta(snapshot, before_declaration)
            if self._public_boundary_container_identity_changed(before_containers):
                snapshot["unsafe_mutation"] = True
            elif hidden_boundary and (
                self._public_boundary_containers_changed(before_containers)
                or self._collect_connector_tasks(snapshot["connector"])
                != before_tasks
            ):
                snapshot["unsafe_mutation"] = True

    @staticmethod
    def _container_matches_snapshot(state: Mapping[str, Any]) -> bool:
        try:
            current = getattr(state["owner"], state["attribute"])
        except BaseException:
            return False
        if current is not state["value"]:
            return False
        if state["kind"] == "dict":
            return dict(current) == state["contents"]
        if state["kind"] == "list":
            return list(current) == state["contents"]
        return set(current) == state["contents"]

    def _public_transaction_changed(self, snapshot: Mapping[str, Any]) -> bool:
        if any(
            snapshot[name]
            for name in (
                "owned_pair_deltas",
                "owned_book_deltas",
                "owned_task_deltas",
            )
        ) or any(
            snapshot[name] is not None
            for name in ("owned_started_delta", "owned_cache_delta")
        ):
            return True
        initial = snapshot["initial_cache_state"]
        if not snapshot["raw_capabilities_copied"]:
            return True
        if self._cached_public_connector(snapshot["connector_key"]) is not initial[
            "cache_connector"
        ]:
            return True
        try:
            current_capabilities = getattr(snapshot["connector"], "api_capabilities")
            if current_capabilities != snapshot["raw_capabilities_contents"]:
                return True
        except BaseException:
            return True
        if any(
            not self._container_matches_snapshot(state)
            for state in snapshot["containers"]
        ):
            return True
        if self._collect_connector_tasks(snapshot["connector"]) != snapshot["tasks"]:
            return True
        connector_name = snapshot["connector_name"]
        started = getattr(self, "_data_connectors_started", {})
        return (
            (connector_name in started) != initial["data_started_present"]
            or started.get(connector_name) != initial["data_started"]
        )

    def _public_transaction_can_restore(
        self, snapshot: Mapping[str, Any]
    ) -> bool:
        initial = snapshot["initial_cache_state"]
        if snapshot["unsafe_mutation"]:
            return False
        if not initial["cache_present"]:
            return False
        if initial["cache_connector"] is not snapshot["connector"]:
            return False
        if not self._public_connector_is_owned(snapshot):
            return False
        if getattr(snapshot["connector"], "order_book_tracker", None) is not snapshot[
            "tracker"
        ]:
            return False
        for delta in (
            *snapshot["owned_pair_deltas"],
            *snapshot["owned_book_deltas"],
        ):
            try:
                if (
                    getattr(delta["owner"], delta["attribute"])
                    is not delta["container"]
                ):
                    return False
            except BaseException:
                return False
        return True

    async def _cancel_public_transaction_tasks(
        self, snapshot: Mapping[str, Any]
    ) -> Set[asyncio.Task]:
        created = set(snapshot["owned_tasks"])
        for task in created:
            if not task.done() and task.cancelling() == 0:
                task.cancel()
        pending = {task for task in created if not task.done()}
        if pending:
            _, pending = await asyncio.wait(
                pending, timeout=_CONNECTOR_CLEANUP_TIMEOUT
            )
        for task in created - pending:
            try:
                task.exception()
            except (asyncio.CancelledError, Exception):
                pass
        return set(pending)

    def _restore_public_transaction_state(
        self, snapshot: Mapping[str, Any]
    ) -> bool:
        restored = True
        for delta in reversed(snapshot["owned_task_deltas"]):
            try:
                owner = delta["owner"]
                attribute = delta["attribute"]
                task = delta["task"]
                if delta["kind"] == "member":
                    container = delta["container"]
                    if getattr(owner, attribute) is not container:
                        continue
                    if isinstance(container, set):
                        container.discard(task)
                    else:
                        for index, value in enumerate(container):
                            if value is task:
                                container.pop(index)
                                break
                    continue
                key = delta["key"]
                if key is None:
                    if getattr(owner, attribute, None) is task:
                        if delta["baseline_present"]:
                            setattr(owner, attribute, delta["baseline_value"])
                        else:
                            delattr(owner, attribute)
                else:
                    mapping = getattr(owner, attribute)
                    if mapping.get(key) is task:
                        if delta["baseline_present"]:
                            mapping[key] = delta["baseline_value"]
                        else:
                            mapping.pop(key, None)
            except BaseException:
                restored = False

        for delta in reversed(snapshot["owned_book_deltas"]):
            try:
                books = delta["container"]
                key = delta["key"]
                if delta["action"] == "remove":
                    if key not in books:
                        books[key] = delta["baseline_value"]
                elif books.get(key) is delta["owned_value"]:
                    if delta["baseline_present"]:
                        books[key] = delta["baseline_value"]
                    else:
                        books.pop(key, None)
            except BaseException:
                restored = False

        for delta in reversed(snapshot["owned_pair_deltas"]):
            try:
                pairs = delta["container"]
                value = delta["value"]
                if (
                    delta["action"] == "remove"
                    and pairs.count(value) == delta["baseline_count"] - 1
                ):
                    pairs.append(value)
                elif (
                    delta["action"] == "add"
                    and pairs.count(value) == delta["baseline_count"] + 1
                ):
                    pairs.remove(value)
                else:
                    restored = False
            except BaseException:
                restored = False

        started_delta = snapshot["owned_started_delta"]
        if started_delta is not None:
            started = self._data_connectors_started
            connector_name = snapshot["connector_name"]
            if started.get(connector_name) is started_delta["owned_value"]:
                if started_delta["baseline_present"]:
                    started[connector_name] = started_delta["baseline_value"]
                else:
                    started.pop(connector_name, None)
        return restored

    def _retain_public_transaction_tasks(
        self,
        snapshot: Mapping[str, Any],
        pending: Set[asyncio.Task],
        disposition: str,
    ) -> ConnectorCapabilityError:
        connector_key = snapshot["connector_key"]
        owner_key = self._owner_key(*connector_key, "public_transaction")
        self._connector_cleanup_owners[owner_key] = {
            "connector": snapshot["connector"],
            "snapshot": snapshot,
            "disposition": disposition,
        }
        self._connector_residual_tasks.setdefault(owner_key, set()).update(pending)
        self._connector_cleanup_phases[owner_key] = {
            "task_drain": {
                "task": next(iter(pending), None),
                "tasks": tuple(pending),
                "complete": not pending,
                "result": True if not pending else None,
                "error_code": (
                    "public_task_cleanup_pending" if pending else None
                ),
                "cancel_requested": bool(pending),
            },
            "disposition": {
                "task": None,
                "complete": False,
                "result": disposition,
                "error_code": None,
                "cancel_requested": False,
            },
        }
        cleanup_code = (
            "public_task_cleanup_pending"
            if pending
            else "public_disposition_pending"
        )
        detail = CapabilityErrorDetail(
            code="connector_capability_cleanup_residual",
            message=(
                f"Rejected capability cleanup is incomplete for "
                f"'{snapshot['connector_name']}'"
            ),
            connector=snapshot["connector_name"],
            feature="capabilities",
            reason="cleanup_incomplete",
            cleanup_scope="connector",
            cleanup_code=cleanup_code,
        )
        self._connector_creation_residuals[connector_key] = detail.model_dump(
            exclude_none=True
        )
        self._signal_market_state_change(connector_key)
        return ConnectorCapabilityError(detail)

    @staticmethod
    def _rejected_capability_cleanup_detail(
        connector_name: str,
        cleanup_code: str,
    ) -> CapabilityErrorDetail:
        """Keep the stable rejected-capability residual at its public boundary."""
        return CapabilityErrorDetail(
            code="connector_capability_cleanup_residual",
            message=(
                f"Rejected capability cleanup is incomplete for "
                f"'{connector_name}'"
            ),
            connector=connector_name,
            feature="capabilities",
            reason="cleanup_incomplete",
            cleanup_scope="connector",
            cleanup_code=cleanup_code,
        )

    async def _discard_public_transaction_connector(
        self,
        owner_key: Tuple[str, str, str],
        snapshot: Mapping[str, Any],
    ) -> None:
        """Let the exact rejected connector own shutdown, then unpublish it."""
        connector = snapshot["connector"]
        account_name, connector_name = snapshot["connector_key"]
        if self._public_connector_is_owned(snapshot):
            if account_name == "__data__":
                detail = await self._finalize_stop_data_connector(
                    connector_name,
                    invoking_owner_key=owner_key,
                )
            else:
                detail = await self._finalize_stop_trading_connector(
                    account_name,
                    connector_name,
                    invoking_owner_key=owner_key,
                )
            if detail is not None:
                if (
                    account_name == "__data__"
                    and detail.code == "connector_cleanup_residual"
                ):
                    detail = self._rejected_capability_cleanup_detail(
                        connector_name,
                        "connector_stop_failed",
                    )
                    self._connector_creation_residuals[
                        snapshot["connector_key"]
                    ] = detail.model_dump(exclude_none=True)
                    self._signal_market_state_change(snapshot["connector_key"])
                raise ConnectorCapabilityError(detail)
            return

        stop_network = getattr(connector, "stop_network", None)
        if stop_network is None:
            raise RuntimeError("connector_stop_unavailable")
        await self._run_owner_operation(
            owner_key, "discard_stop", stop_network
        )

    async def _complete_public_transaction_disposition(
        self, owner_key: Tuple[str, str, str]
    ) -> None:
        owner = self._connector_cleanup_owners[owner_key]
        snapshot = owner["snapshot"]
        disposition = owner["disposition"]
        phase = self._connector_cleanup_phases[owner_key]["disposition"]
        if disposition == "restore":
            if self._public_transaction_can_restore(
                snapshot
            ) and self._restore_public_transaction_state(snapshot):
                phase["complete"] = True
                phase["result"] = "restore"
                return
            disposition = "discard"
            owner["disposition"] = disposition
            phase["result"] = disposition
        await self._discard_public_transaction_connector(owner_key, snapshot)
        phase["complete"] = True
        phase["result"] = "discard"

    async def _retry_public_transaction_cleanup(
        self, connector_key: Tuple[str, str]
    ) -> None:
        owner_key = self._owner_key(*connector_key, "public_transaction")
        restart_owner_key = self._owner_key(*connector_key, "restart")
        if (
            owner_key not in self._connector_cleanup_owners
            and any(
                restart_owner_key in registry
                for registry in (
                    self._connector_cleanup_owners,
                    self._connector_residual_tasks,
                    self._connector_cleanup_phases,
                )
            )
        ):
            account_name, connector_name = connector_key
            cached_connector = self._cached_public_connector(connector_key)
            if cached_connector is not None:
                try:
                    self._validate_instance_capabilities(
                        connector_name, cached_connector
                    )
                except ConnectorCapabilityError:
                    return
            if account_name == "__data__":
                detail = await self._finalize_stop_data_connector(connector_name)
            else:
                detail = await self._finalize_stop_trading_connector(
                    account_name, connector_name
                )
            if detail is not None:
                raise ConnectorCapabilityError(detail)
        if owner_key not in self._connector_cleanup_owners:
            return
        if not await self._drain_owner_tasks(owner_key):
            residual = self._connector_creation_residuals[connector_key]
            raise ConnectorCapabilityError(
                CapabilityErrorDetail.model_validate(residual)
            )
        phase = self._connector_cleanup_phases.get(owner_key, {}).get("task_drain")
        if phase is not None:
            phase["complete"] = True
            phase["error_code"] = None
            phase["result"] = True
        try:
            await self._complete_public_transaction_disposition(owner_key)
        except asyncio.CancelledError:
            raise
        except ConnectorCapabilityError:
            raise
        except BaseException as exc:
            disposition_phase = self._connector_cleanup_phases.get(
                owner_key, {}
            ).get("disposition")
            if disposition_phase is not None:
                disposition_phase["error_code"] = "public_disposition_pending"
            detail = CapabilityErrorDetail(
                code="connector_capability_cleanup_residual",
                message=(
                    f"Rejected capability cleanup is incomplete for "
                    f"'{connector_key[1]}'"
                ),
                connector=connector_key[1],
                feature="capabilities",
                reason="cleanup_incomplete",
                cleanup_scope="connector",
                cleanup_code="public_disposition_pending",
            )
            self._connector_creation_residuals[connector_key] = (
                detail.model_dump(exclude_none=True)
            )
            self._signal_market_state_change(connector_key)
            logger.error(
                "Public transaction cleanup failed for %s/%s (%s)",
                connector_key[0],
                connector_key[1],
                type(exc).__name__,
            )
            raise ConnectorCapabilityError(detail) from None
        self._finish_owner_cleanup(owner_key)
        self._connector_creation_residuals.pop(connector_key, None)

    def _validate_public_transaction_state(
        self, snapshot: Mapping[str, Any]
    ) -> None:
        self._require_public_connector_ownership(snapshot)
        self._validate_public_data_capabilities(
            snapshot["connector_name"],
            snapshot["connector"],
            snapshot["trading_pair"],
        )

    async def _cleanup_public_transaction(
        self,
        snapshot: Mapping[str, Any],
        *,
        force_discard: bool,
    ) -> None:
        changed = self._public_transaction_changed(snapshot)
        if not changed and not force_discard:
            return
        pending = await self._cancel_public_transaction_tasks(snapshot)
        can_restore = (
            not force_discard
            and self._public_transaction_can_restore(snapshot)
        )
        if pending:
            raise self._retain_public_transaction_tasks(
                snapshot,
                pending,
                "restore" if can_restore else "discard",
            )
        if can_restore and self._restore_public_transaction_state(snapshot):
            return
        self._retain_public_transaction_tasks(snapshot, set(), "discard")
        await self._retry_public_transaction_cleanup(snapshot["connector_key"])

    async def _await_public_transaction_cleanup(
        self,
        snapshot: Mapping[str, Any],
        *,
        force_discard: bool,
        cancellation_args: Optional[Tuple[Any, ...]] = None,
    ) -> None:
        """Finish rollback under shield and then reproduce caller cancellation."""
        cleanup_task = asyncio.create_task(
            self._cleanup_public_transaction(
                snapshot, force_discard=force_discard
            )
        )
        connector_key = snapshot["connector_key"]
        lease = self._active_market_transaction_lease(connector_key)
        if lease is not None and not self._retain_market_transaction_descendant(
            connector_key, cleanup_task
        ):
            cleanup_task.cancel()
            await asyncio.gather(cleanup_task, return_exceptions=True)
            raise RuntimeError("public_cleanup_authority_unavailable")
        preserved_args = cancellation_args
        while not cleanup_task.done():
            try:
                await asyncio.shield(cleanup_task)
            except asyncio.CancelledError as cancellation:
                if cleanup_task.done():
                    break
                if preserved_args is None:
                    preserved_args = cancellation.args
            except BaseException:
                break
        cleanup_error = None
        try:
            cleanup_task.result()
        except BaseException as exc:
            cleanup_error = exc
        if preserved_args is not None:
            raise asyncio.CancelledError(*preserved_args)
        if cleanup_error is not None:
            raise cleanup_error

    @staticmethod
    def _public_boundary_error(
        snapshot: Mapping[str, Any], boundary: str
    ) -> ConnectorCapabilityError:
        connector_name = snapshot["connector_name"]
        trading_pair = snapshot["trading_pair"]
        if boundary == "pair":
            code = "connector_pair_registration_failed"
            message = (
                f"Connector '{connector_name}' could not register trading pair "
                f"'{trading_pair}'"
            )
            reason = "pair_registration_failed"
        elif boundary == "book_wait":
            code = "connector_order_book_wait_failed"
            message = (
                f"Connector '{connector_name}' could not initialize order book "
                f"for '{trading_pair}'"
            )
            reason = "order_book_wait_failed"
        else:
            code = "connector_symbol_discovery_failed"
            message = (
                f"Connector '{connector_name}' could not validate trading pair "
                f"'{trading_pair}'"
            )
            reason = "symbol_discovery_failed"
        return ConnectorCapabilityError(
            CapabilityErrorDetail(
                code=code,
                message=message,
                connector=connector_name,
                feature=ConnectorFeature.LIVE_ORDER_BOOK.value,
                trading_pair=trading_pair,
                reason=reason,
            )
        )

    async def _run_public_initialization_boundary(
        self,
        snapshot: Mapping[str, Any],
        boundary: str,
        operation,
    ):
        """Validate and roll back every await outcome, including cancellation."""
        try:
            result = await self._run_public_operation_with_ledger(
                snapshot, boundary, operation
            )
        except asyncio.CancelledError as cancellation:
            validation_error = None
            try:
                self._validate_public_transaction_state(snapshot)
            except ConnectorCapabilityError as exc:
                validation_error = exc
            await self._await_public_transaction_cleanup(
                snapshot,
                force_discard=validation_error is not None,
                cancellation_args=cancellation.args,
            )
            raise AssertionError("cancellation cleanup must re-raise")
        except BaseException as exc:
            validation_error = None
            try:
                self._validate_public_transaction_state(snapshot)
            except ConnectorCapabilityError as invalid:
                validation_error = invalid
            await self._await_public_transaction_cleanup(
                snapshot,
                force_discard=(
                    validation_error is not None or boundary == "discovery"
                ),
            )
            if boundary == "book_wait" and isinstance(exc, Exception):
                raise self._public_boundary_error(snapshot, boundary) from None
            if isinstance(exc, ConnectorCapabilityError):
                raise exc
            if validation_error is not None:
                raise validation_error
            if boundary == "discovery" and isinstance(exc, Exception):
                raise self._public_boundary_error(snapshot, boundary) from None
            if boundary == "pair" and isinstance(exc, Exception):
                raise self._public_boundary_error(snapshot, boundary) from None
            if boundary == "start" and isinstance(exc, Exception):
                logger.error(
                    "Error starting data connector %s (%s)",
                    snapshot["connector_name"],
                    type(exc).__name__,
                )
                return False
            raise

        validation_error = None
        try:
            self._validate_public_transaction_state(snapshot)
        except ConnectorCapabilityError as exc:
            validation_error = exc
        if validation_error is not None:
            await self._await_public_transaction_cleanup(
                snapshot, force_discard=True
            )
            raise validation_error
        if result is False:
            await self._await_public_transaction_cleanup(
                snapshot, force_discard=False
            )
        return result

    def _validate_public_data_capabilities(
        self,
        connector_name: str,
        connector: ConnectorBase,
        trading_pair: str,
    ) -> ConnectorCapabilities:
        capabilities = self._validate_instance_capabilities(
            connector_name, connector
        )
        for feature in (
            ConnectorFeature.DYNAMIC_SUBSCRIPTION,
            ConnectorFeature.LIVE_ORDER_BOOK,
        ):
            self._capability_registry.require_resolved(
                connector_name, capabilities, feature, trading_pair
            )
        return capabilities

    def _public_connector_is_owned(self, snapshot: Mapping[str, Any]) -> bool:
        connector_key = snapshot["connector_key"]
        if self._connector_incarnation(connector_key) != snapshot["incarnation"]:
            return False
        account_name, connector_name = connector_key
        if account_name == "__data__":
            current = getattr(self, "_data_connectors", {}).get(connector_name)
        else:
            current = getattr(self, "_trading_connectors", {}).get(
                account_name, {}
            ).get(
                connector_name
            )
        return current is snapshot["connector"]

    def _require_public_connector_ownership(
        self, snapshot: Mapping[str, Any]
    ) -> None:
        if self._public_connector_is_owned(snapshot):
            return
        connector_name = snapshot["connector_name"]
        raise ConnectorCapabilityError(
            CapabilityErrorDetail(
                code="connector_lifecycle_changed",
                message=(
                    f"Connector '{connector_name}' changed while the request was "
                    "initializing public data"
                ),
                connector=connector_name,
                feature=ConnectorFeature.LIVE_ORDER_BOOK.value,
                reason="connector_incarnation_changed",
            )
        )

    async def _is_trading_pair_supported(
        self,
        connector_name: str,
        connector: ConnectorBase,
        trading_pair: str
    ) -> bool:
        """Check that a trading pair exists in the connector's symbol map.

        Registering an unknown pair poisons the order book WebSocket loop for every
        other pair: the data source iterates over all registered pairs when
        subscribing, so a single unknown pair raises and aborts the whole
        subscription, which then retries and fails forever.
        """
        try:
            missing = object()
            descriptor = inspect.getattr_static(
                connector, "exchange_symbol_associated_to_pair", missing
            )
            if descriptor is missing:
                if connector_name == "manzana":
                    raise AttributeError("required symbol discovery is unavailable")
                return True
            discovery = getattr(connector, "exchange_symbol_associated_to_pair")
            if not callable(discovery):
                raise TypeError("symbol discovery is not callable")
            await discovery(trading_pair=trading_pair)
            return True
        except Exception as exc:
            logger.warning(
                "Symbol discovery failed for %s/%s (%s)",
                connector_name,
                trading_pair,
                type(exc).__name__,
            )
            raise ConnectorCapabilityError(
                CapabilityErrorDetail(
                    code="connector_symbol_discovery_failed",
                    message=(
                        f"Connector '{connector_name}' could not validate trading pair "
                        f"'{trading_pair}'"
                    ),
                    connector=connector_name,
                    feature=ConnectorFeature.DYNAMIC_SUBSCRIPTION.value,
                    trading_pair=trading_pair,
                    reason="symbol_discovery_failed",
                )
            ) from None

    async def _discard_invalid_public_declaration(
        self,
        connector_name: str,
        connector: ConnectorBase,
        approved_state,
    ) -> None:
        """Unpublish rejected truth unless exact restoration is provably safe."""
        if isinstance(approved_state, Mapping) and "connector_key" in approved_state:
            snapshot = approved_state
        else:
            connector_key = self._resolve_public_connector_key(connector_name, None)
            snapshot = self._snapshot_public_connector_state(
                connector_name,
                connector,
                connector_key,
                approved_state,
            )

        if not self._public_connector_is_owned(snapshot):
            return

        connector_key = snapshot["connector_key"]
        account_name, _ = connector_key
        if account_name == "__data__":
            cleanup_detail = await self._finalize_stop_data_connector(connector_name)
        else:
            cleanup_detail = await self._finalize_stop_trading_connector(
                account_name, connector_name
            )
        if cleanup_detail is not None:
            if cleanup_detail.code == "connector_cleanup_residual":
                cleanup_detail = self._rejected_capability_cleanup_detail(
                    connector_name,
                    "connector_stop_failed",
                )
                self._connector_creation_residuals[connector_key] = (
                    cleanup_detail.model_dump(exclude_none=True)
                )
                self._signal_market_state_change(connector_key)
            raise ConnectorCapabilityError(cleanup_detail)

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
        """Serialize the complete public-data transaction under its physical owner."""
        while True:
            connector_key = self._resolve_public_connector_key(
                connector_name, account_name
            )
            async with self._market_transaction_lock(
                connector_key, trading_pair
            ):
                async with self._connector_creation_lock(connector_key):
                    if (
                        self._resolve_public_connector_key(
                            connector_name, account_name
                        )
                        != connector_key
                    ):
                        continue
                    await self._retry_public_transaction_cleanup(connector_key)
                    residual = self._connector_creation_residuals.get(connector_key)
                    if residual is not None:
                        raise ConnectorCapabilityError(
                            CapabilityErrorDetail.model_validate(residual)
                        )
                    return await self._initialize_order_book_locked(
                        connector_name,
                        trading_pair,
                        connector_key,
                        timeout,
                    )

    async def _initialize_order_book_locked(
        self,
        connector_name: str,
        trading_pair: str,
        connector_key: Tuple[str, str],
        timeout: float,
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
        bootstrap_capabilities = self._capability_registry.get(connector_name)
        for feature in (
            ConnectorFeature.DYNAMIC_SUBSCRIPTION,
            ConnectorFeature.LIVE_ORDER_BOOK,
        ):
            self._capability_registry.require_resolved(
                connector_name, bootstrap_capabilities, feature, trading_pair
            )

        initial_cache_state = self._capture_public_cache_state(connector_key)
        connector = self._connector_for_public_key(connector_key)

        if not connector:
            logger.error(f"No connector available for {connector_name}")
            return False

        rejected_snapshot = self._snapshot_public_connector_state(
            connector_name,
            connector,
            connector_key,
            bootstrap_capabilities,
            initial_cache_state=initial_cache_state,
            trading_pair=trading_pair,
        )
        try:
            approved_capabilities = self._validate_public_data_capabilities(
                connector_name, connector, trading_pair
            )
        except ConnectorCapabilityError:
            await self._discard_invalid_public_declaration(
                connector_name, connector, rejected_snapshot
            )
            raise

        # Gateway/AMM connectors do not expose an order-book tracker. The loaded
        # Manzana declaration has already been validated above this legacy no-op.
        if (
            not hasattr(connector, "order_book_tracker")
            or connector.order_book_tracker is None
        ):
            logger.info(
                "Connector %s doesn't have order book tracker "
                "(AMM/Gateway) - skipping",
                connector_name,
            )
            return True

        snapshot = rejected_snapshot
        snapshot["approved_capabilities"] = approved_capabilities

        tracker = connector.order_book_tracker

        # Check if already initialized
        if trading_pair in tracker.order_books:
            ob = tracker.order_books[trading_pair]
            try:
                bids, asks = ob.snapshot
                if len(bids) > 0 and len(asks) > 0:
                    logger.info(f"Order book for {trading_pair} already initialized")
                    self._require_public_connector_ownership(snapshot)
                    return True
            except Exception:
                pass

        # Reject pairs the exchange doesn't list before registering them anywhere
        pair_is_supported = await self._run_public_initialization_boundary(
            snapshot,
            "discovery",
            self._is_trading_pair_supported(
                connector_name, connector, trading_pair
            ),
        )
        if not pair_is_supported:
            logger.error(
                f"Trading pair {trading_pair} is not listed on {connector_name} - "
                f"refusing to register it (check the quote asset)"
            )
            return False

        # For data connectors, ensure network is started
        if connector_key[0] == "__data__":
            if not self._data_connectors_started.get(connector_name, False):
                success = await self._run_public_initialization_boundary(
                    snapshot,
                    "start",
                    self._ensure_data_connector_started_locked(
                        connector_name, trading_pair, connector, snapshot
                    ),
                )
                if not success:
                    return False
                # Wait for order book after starting
                ready = await self._run_public_initialization_boundary(
                    snapshot,
                    "book_wait",
                    self._wait_for_order_book(tracker, trading_pair, timeout),
                )
                if not ready:
                    return False
                return True
            else:
                # Connector started, dynamically add trading pair
                success = await self._run_public_initialization_boundary(
                    snapshot,
                    "pair",
                    self._add_trading_pair_to_tracker(connector, trading_pair),
                )
                if not success:
                    return False

        # For trading connectors, dynamically add trading pair
        else:
            success = await self._run_public_initialization_boundary(
                snapshot,
                "pair",
                self._add_trading_pair_to_tracker(connector, trading_pair),
            )
            if not success:
                return False

        # Wait for order book to have data
        ready = await self._run_public_initialization_boundary(
            snapshot,
            "book_wait",
            self._wait_for_order_book(tracker, trading_pair, timeout),
        )
        if not ready:
            return False
        return True

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
            except Exception as exc:
                logger.error(
                    "dynamic_subscription_snapshot_failed for %s/%s (%s)",
                    type(connector).__name__,
                    trading_pair,
                    type(exc).__name__,
                )

            logger.error(f"Failed to add {trading_pair} to order book tracker")
            self._purge_trading_pair_registration(connector, trading_pair)
            return False

        except Exception as exc:
            logger.error(
                "dynamic_subscription_add_failed for %s/%s (%s)",
                type(connector).__name__,
                trading_pair,
                type(exc).__name__,
            )
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
        while True:
            connector_key = self._resolve_public_connector_key(
                connector_name, account_name
            )
            async with self._market_transaction_lock(
                connector_key, trading_pair
            ):
                if (
                    self._resolve_public_connector_key(
                        connector_name, account_name
                    )
                    != connector_key
                ):
                    continue
                return await self._remove_trading_pair_already_locked(
                    connector_name, trading_pair, connector_key
                )

    async def _remove_trading_pair_already_locked(
        self,
        connector_name: str,
        trading_pair: str,
        connector_key: Tuple[str, str],
    ) -> bool:
        """Remove one pair under an existing exact-root or retained physical lease."""
        async with self._connector_creation_lock(connector_key):
            await self._retry_public_transaction_cleanup(connector_key)
            residual = self._connector_creation_residuals.get(connector_key)
            if residual is not None:
                raise ConnectorCapabilityError(
                    CapabilityErrorDetail.model_validate(residual)
                )
            connector = self._connector_for_public_key(connector_key)
            if not connector:
                logger.warning(
                    "No connector available for %s to remove %s",
                    connector_name,
                    trading_pair,
                )
                return False
            if self._connector_incarnation(connector_key) is None:
                self._advance_connector_incarnation(connector_key)
            incarnation = self._connector_incarnation(connector_key)
            result = await self._remove_trading_pair_from_tracker(
                connector, trading_pair
            )
            if not result:
                return False
            if (
                self._connector_incarnation(connector_key) != incarnation
                or self._cached_public_connector(connector_key) is not connector
            ):
                logger.warning(
                    "Connector lifecycle changed while removing %s from %s",
                    trading_pair,
                    connector_name,
                )
                return False
            return True

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
        connector = self._create_trading_connector(account_name, connector_name)
        creation_owner["connector"] = connector
        await self._initialize_trading_connector_candidate(
            connector, account_name, connector_name
        )
        await self._activate_trading_connector_candidate(
            connector, account_name, connector_name
        )
        return connector

    async def _initialize_trading_connector_candidate(
        self,
        connector: ConnectorBase,
        account_name: str,
        connector_name: str,
    ) -> None:
        """Validate credentials and remote state without publishing the candidate."""
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

    async def _activate_trading_connector_candidate(
        self,
        connector: ConnectorBase,
        account_name: str,
        connector_name: str,
        staged_indices: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Start private runtime state before one synchronous publication."""

        # Load existing orders from database
        if self.db_manager:
            await self._load_existing_orders(connector, account_name, connector_name)

        # Setup order and funding recorders
        cache_key = f"{account_name}:{connector_name}"
        orders_missing = (
            cache_key not in self._orders_recorders
            if staged_indices is None
            else "orders" not in staged_indices
        )
        if self.db_manager and orders_missing:
            from services.orders_recorder import OrdersRecorder
            orders_recorder = OrdersRecorder(self.db_manager, account_name, connector_name)
            if staged_indices is not None:
                staged_indices["orders"] = orders_recorder
            orders_recorder.start(connector)
            if staged_indices is None:
                self._orders_recorders[cache_key] = orders_recorder

            if self._is_perpetual_connector(connector):
                from services.funding_recorder import FundingRecorder
                funding_recorder = FundingRecorder(self.db_manager, account_name, connector_name)
                if staged_indices is not None:
                    staged_indices["funding"] = funding_recorder
                funding_recorder.start(connector)
                if staged_indices is None:
                    self._funding_recorders[cache_key] = funding_recorder

        # Initialize metrics
        if staged_indices is None:
            self._initialize_metrics(
                connector, account_name, connector_name, cache_key
            )
        else:
            self._initialize_metrics(
                connector,
                account_name,
                connector_name,
                cache_key,
                staged_indices=staged_indices,
            )

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
        retained_index = self._connector_residual_tasks.get(owner_key)
        retained_tasks = set(retained_index or ())
        owned = new_tasks | retained_tasks
        pending = {task for task in owned if task is not current and not task.done()}
        if pending:
            retained_index = self._connector_residual_tasks.setdefault(
                owner_key, set()
            )
            retained_index.update(pending)
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
            return False
        if retained_index is not None:
            for task in owned:
                if task.done():
                    retained_index.discard(task)
            if (
                not retained_index
                and self._connector_residual_tasks.get(owner_key) is retained_index
            ):
                self._connector_residual_tasks.pop(owner_key, None)
        for task in owned:
            if task.done():
                try:
                    task.exception()
                except (asyncio.CancelledError, Exception):
                    pass
        return True

    def _unpublish_terminal_connector(
        self,
        owner_key: Tuple[str, str, str],
        phase: Mapping[str, Any],
    ) -> None:
        """Remove only the exact physical connector whose stop is terminal."""
        connector_key = owner_key[:2]
        authority = phase.get("authority", _OWNER_AUTHORITY_UNSET)
        incarnation = phase.get("authority_incarnation", _OWNER_AUTHORITY_UNSET)
        if authority is _OWNER_AUTHORITY_UNSET:
            return
        if (
            incarnation is not _OWNER_AUTHORITY_UNSET
            and self._connector_incarnation(connector_key) != incarnation
        ):
            return
        if self._cached_public_connector(connector_key) is not authority:
            return
        account_name, connector_name = connector_key
        if account_name == "__data__":
            self._data_connectors.pop(connector_name, None)
            self._data_connectors_started.pop(connector_name, None)
            return
        account_connectors = self._trading_connectors.get(account_name)
        if account_connectors is not None:
            account_connectors.pop(connector_name, None)
            if not account_connectors:
                self._trading_connectors.pop(account_name, None)

    def _prune_terminal_connector_state(
        self,
        connector_key: Tuple[str, str],
        owner_key: Tuple[str, str, str],
        authority,
    ) -> None:
        """Synchronously forget private truth owned by one terminal generation."""
        if self._cached_public_connector(connector_key) is not None:
            return

        self._finish_owner_cleanup(owner_key)
        self._connector_creation_residuals.pop(connector_key, None)
        for market_key in tuple(self._market_cleanup_phases):
            if market_key[:2] == connector_key and market_key[2:3] == (
                "remote_cleanup",
            ):
                self._market_cleanup_phases.pop(market_key, None)
                self._market_residual_tasks.pop(market_key, None)
        for market_key, market_owner in tuple(
            self._market_cleanup_owners.items()
        ):
            owned_connector = (
                market_owner.get("connector")
                if isinstance(market_owner, dict)
                else None
            )
            if market_key[:2] == connector_key and owned_connector is authority:
                self._market_cleanup_owners.pop(market_key, None)
                self._market_cleanup_residuals.pop(market_key, None)
        if not self._connector_has_private_state(connector_key):
            self._connector_incarnations.pop(connector_key, None)

    async def _reconcile_owner_operation_terminal(
        self,
        owner_key: Tuple[str, str, str],
        phase_name: str,
        phase: Dict[str, Any],
    ) -> None:
        """Commit terminal owner, phase, and cache truth before lease release."""
        task = asyncio.current_task()
        retained = self._connector_residual_tasks.get(owner_key)
        if retained is not None and task is not None:
            retained.discard(task)
            if not retained:
                self._connector_residual_tasks.pop(owner_key, None)
        if phase_name != "connector_stop":
            return

        authority = phase.get("authority", _OWNER_AUTHORITY_UNSET)
        if authority is not _OWNER_AUTHORITY_UNSET:
            tasks = set(phase.get("terminal_tasks", ()))
            if not await self._drain_owner_tasks(owner_key, tasks):
                if phase.get("error_code") is None:
                    phase["error_code"] = "task_cleanup_pending"
        connector_key = owner_key[:2]
        error_code = phase.get("error_code")
        for market_key, market_phase in tuple(
            self._market_cleanup_phases.items()
        ):
            if (
                market_key[:2] != connector_key
                or len(market_key) < 4
                or market_key[2] != "remote_cleanup"
                or not isinstance(market_phase, dict)
            ):
                continue
            market_phase["complete"] = True
            market_phase["result"] = phase.get("result")
            market_phase["error_code"] = error_code
            retained_market = self._market_residual_tasks.get(market_key)
            if retained_market is not None:
                for retained_task in tuple(retained_market):
                    if retained_task.done():
                        retained_market.discard(retained_task)
                        self._consume_background_task_result(retained_task)
                if not retained_market:
                    self._market_residual_tasks.pop(market_key, None)

        stopped_indices = list(phase.get("stopped_indices", ()))
        terminal_success = error_code is None and phase.get("result") is not False
        if terminal_success:
            if phase.get(
                "commit_all_on_success", True
            ) and not self._has_auxiliary_stop_state(connector_key, owner_key):
                self._commit_terminal_connector_stop(
                    connector_key, authority, stopped_indices
                )
            else:
                self._unpublish_terminal_connector(owner_key, phase)
                cache_key = self._index_key(*connector_key)
                mappings = {
                    "orders": self._orders_recorders,
                    "funding": self._funding_recorders,
                    "metrics": self._metrics_collectors,
                }
                for label, value in stopped_indices:
                    mapping = mappings[label]
                    if mapping.get(cache_key) is value:
                        mapping.pop(cache_key, None)
                self._connector_cleanup_indices.pop(connector_key, None)
                if authority is not _OWNER_AUTHORITY_UNSET:
                    try:
                        setattr(authority, "_api_network_stop_terminal", True)
                    except Exception:
                        pass
                self._prune_terminal_connector_state(
                    connector_key, owner_key, authority
                )
                self._signal_market_state_change(connector_key)
            phase["pre_stop_reconciled"] = True
            self._signal_market_state_change(connector_key)
            return

        rollback_code = phase.get("rollback_code")
        if not phase.get("pre_stop_reconciled"):
            rollback_code = await self._resume_pre_stop_indices(
                connector_key, stopped_indices
            )
            phase["rollback_code"] = rollback_code
            phase["pre_stop_reconciled"] = True
        self._retain_connector_cleanup_residual(
            connector_key,
            rollback_code or error_code or "connector_cleanup_incomplete",
            phase.get("retained_indices", {}),
        )

    async def _owner_operation_wrapper(
        self,
        owner_key: Tuple[str, str, str],
        phase_name: str,
        phase: Dict[str, Any],
        awaitable,
    ):
        """Keep retained authority until terminal truth has been reconciled."""
        try:
            result = await awaitable
        except asyncio.CancelledError:
            phase["complete"] = True
            phase["error_code"] = f"{phase_name}_cancelled"
            await self._reconcile_owner_operation_terminal(
                owner_key, phase_name, phase
            )
            return None
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
            await self._reconcile_owner_operation_terminal(
                owner_key, phase_name, phase
            )
            return None
        phase["complete"] = True
        phase["result"] = result
        if result is False:
            phase["error_code"] = f"{phase_name}_returned_false"
        await self._reconcile_owner_operation_terminal(
            owner_key, phase_name, phase
        )
        return result

    async def _run_owner_operation(
        self,
        owner_key: Tuple[str, str, str],
        phase_name: str,
        operation,
        *,
        timeout: float = _CONNECTOR_CLEANUP_TIMEOUT,
        authority=_OWNER_AUTHORITY_UNSET,
        authority_incarnation=_OWNER_AUTHORITY_UNSET,
        terminal_tasks: Optional[Set[asyncio.Task]] = None,
        terminal_context: Optional[Mapping[str, Any]] = None,
    ):
        """Run one non-repeatable phase through a retained terminal wrapper."""
        self._ensure_connector_cleanup_state()
        owner_phases = self._connector_cleanup_phases.setdefault(owner_key, {})
        phase = owner_phases.get(phase_name)
        if phase is None:
            phase = {
                "task": None,
                "complete": False,
                "result": None,
                "error_code": None,
                "cancel_requested": False,
            }
            if authority is not _OWNER_AUTHORITY_UNSET:
                phase["authority"] = authority
                phase["authority_incarnation"] = authority_incarnation
                phase["terminal_tasks"] = tuple(terminal_tasks or ())
            if phase_name == "connector_stop":
                terminal_context = terminal_context or {}
                phase["stopped_indices"] = tuple(
                    terminal_context.get("stopped_indices", ())
                )
                phase["retained_indices"] = dict(
                    terminal_context.get("retained_indices", {})
                )
                phase["commit_all_on_success"] = terminal_context.get(
                    "commit_all_on_success", True
                )
                phase["pre_stop_reconciled"] = False
                phase["rollback_code"] = None
            owner_phases[phase_name] = phase
        elif authority is not _OWNER_AUTHORITY_UNSET:
            if (
                phase.get("authority", _OWNER_AUTHORITY_UNSET) is not authority
                or phase.get(
                    "authority_incarnation", _OWNER_AUTHORITY_UNSET
                ) != authority_incarnation
            ):
                raise RuntimeError(f"{phase_name}_authority_mismatch")
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
                await self._reconcile_owner_operation_terminal(
                    owner_key, phase_name, phase
                )
                raise RuntimeError(phase["error_code"]) from None
            if not inspect.isawaitable(result):
                phase["complete"] = True
                phase["result"] = result
                if result is False:
                    phase["error_code"] = f"{phase_name}_returned_false"
                await self._reconcile_owner_operation_terminal(
                    owner_key, phase_name, phase
                )
                if phase["error_code"] is not None:
                    raise RuntimeError(phase["error_code"])
                return result
            task = asyncio.create_task(
                self._owner_operation_wrapper(
                    owner_key, phase_name, phase, result
                )
            )
            phase["task"] = task
            connector_key = owner_key[:2]
            lease = self._active_market_transaction_lease(connector_key)
            if lease is not None and not self._retain_market_transaction_descendant(
                connector_key, task
            ):
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                close_awaitable = getattr(result, "close", None)
                if callable(close_awaitable):
                    close_awaitable()
                phase["task"] = None
                phase["complete"] = True
                phase["error_code"] = f"{phase_name}_authority_unavailable"
                raise RuntimeError(phase["error_code"])
            self._connector_residual_tasks.setdefault(owner_key, set()).add(task)

        if not task.done():
            try:
                _, pending = await asyncio.wait({task}, timeout=timeout)
            except asyncio.CancelledError:
                if (
                    phase_name == "connector_stop"
                    and not phase["cancel_requested"]
                    and task.cancelling() == 0
                ):
                    task.cancel()
                    phase["cancel_requested"] = True
                self._connector_residual_tasks.setdefault(owner_key, set()).add(task)
                raise
            if pending:
                if not phase["cancel_requested"] and task.cancelling() == 0:
                    task.cancel()
                    phase["cancel_requested"] = True
                self._connector_residual_tasks.setdefault(owner_key, set()).add(task)
                raise RuntimeError(f"{phase_name}_pending")

        try:
            task.result()
        except (asyncio.CancelledError, Exception):
            pass
        if phase["error_code"] is not None:
            raise RuntimeError(phase["error_code"])
        return phase["result"]

    def _finish_owner_cleanup(self, owner_key: Tuple[str, str, str]) -> None:
        """Discard phase truth only after the complete owner transaction succeeds."""
        self._connector_cleanup_phases.pop(owner_key, None)
        self._connector_residual_tasks.pop(owner_key, None)
        self._connector_cleanup_owners.pop(owner_key, None)

    @staticmethod
    def _orphan_phase_is_terminal_success(
        phase_name: str, phase: Mapping[str, Any]
    ) -> bool:
        """Inspect retained phase truth without replaying an owner operation."""
        if not isinstance(phase, dict):
            return False

        tasks = []
        task = phase.get("task")
        if task is not None:
            tasks.append(task)
        phase_tasks = phase.get("tasks", ())
        if isinstance(phase_tasks, (tuple, list, set)):
            tasks.extend(item for item in phase_tasks if item not in tasks)

        for retained_task in tasks:
            done = getattr(retained_task, "done", None)
            result = getattr(retained_task, "result", None)
            if not callable(done) or not callable(result) or not done():
                return False
            try:
                task_result = result()
            except asyncio.CancelledError:
                phase["complete"] = True
                phase["error_code"] = f"{phase_name}_cancelled"
                return False
            except Exception:
                phase["complete"] = True
                phase["error_code"] = f"{phase_name}_raised"
                return False
            if task_result is False:
                phase["complete"] = True
                phase["result"] = False
                phase["error_code"] = f"{phase_name}_returned_false"
                return False
            phase["result"] = task_result

        if tasks:
            phase["complete"] = True
            if str(phase.get("error_code", "")).endswith("_pending"):
                phase["error_code"] = None

        if phase.get("error_code") is not None:
            return False
        if phase.get("complete"):
            return phase.get("result") is not False
        if phase.get("result") is not None or phase.get("cancel_requested"):
            return False
        return not tasks

    def _reconcile_orphan_connector_phases(
        self, connector_key: Tuple[str, str]
    ) -> list[str]:
        """Consume authority-free terminal phase markers and retain real work."""
        cleanup_codes = []
        owners = self._connector_cleanup_owners
        residual_tasks = self._connector_residual_tasks
        phases = self._connector_cleanup_phases
        market_owners = getattr(self, "_market_cleanup_owners", {})
        has_connector_authority = (
            self._cached_public_connector(connector_key) is not None
            or connector_key in self._connector_creation_residuals
            or any(key[:2] == connector_key for key in owners)
            or any(key[:2] == connector_key for key in market_owners)
        )

        for owner_key in list(phases):
            if owner_key[:2] != connector_key or owner_key in owners:
                continue
            owner_phases = phases.get(owner_key)
            terminal = (
                not has_connector_authority
                and owner_key not in residual_tasks
                and isinstance(owner_phases, dict)
                and all(
                    self._orphan_phase_is_terminal_success(name, phase)
                    for name, phase in owner_phases.items()
                )
            )
            if not terminal:
                cleanup_codes.append(f"{owner_key[2]}_cleanup_incomplete")
                continue
            phases.pop(owner_key, None)
            logger.info(
                "Connector cleanup audit code=orphan_terminal_phase_consumed "
                "account=%s connector=%s owner=%s",
                owner_key[0],
                owner_key[1],
                owner_key[2],
            )
        return cleanup_codes

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

    async def _resume_pre_stop_indices(
        self,
        connector_key: Tuple[str, str],
        stopped_indices: list[Tuple[str, Any]],
    ) -> Optional[str]:
        """Restore stopped auxiliary publishers while the connector stays live."""
        connector = self._cached_public_connector(connector_key)
        rollback_codes = []
        for label, value in reversed(stopped_indices):
            try:
                event_pairs = getattr(value, "_event_pairs", None)
                if (
                    label in {"orders", "funding"}
                    and connector is not None
                    and event_pairs is not None
                ):
                    get_listeners = getattr(connector, "get_listeners", None)
                    add_listener = getattr(connector, "add_listener", None)
                    if callable(add_listener):
                        for event, forwarder in event_pairs:
                            listeners = (
                                get_listeners(event)
                                if callable(get_listeners)
                                else ()
                            )
                            if forwarder not in listeners:
                                add_listener(event, forwarder)
                        continue
                start = getattr(value, "start", None)
                if start is None:
                    continue
                result = (
                    start(connector)
                    if label in {"orders", "funding"}
                    else start()
                )
                if inspect.isawaitable(result):
                    await result
            except BaseException:
                logger.exception(
                    "Failed to roll back %s cleanup for %s/%s",
                    label,
                    connector_key[0],
                    connector_key[1],
                )
                rollback_codes.append(f"{label}_rollback_incomplete")
        return rollback_codes[0] if rollback_codes else None

    async def _resume_pre_stop_indices_before_raise(
        self,
        connector_key: Tuple[str, str],
        stopped_indices: list[Tuple[str, Any]],
    ) -> Optional[str]:
        """Finish rollback even when the transaction task is being cancelled."""
        rollback_task = asyncio.create_task(
            self._resume_pre_stop_indices(connector_key, stopped_indices)
        )
        while not rollback_task.done():
            try:
                await asyncio.shield(rollback_task)
            except asyncio.CancelledError:
                continue
        return rollback_task.result()

    def _retain_connector_cleanup_residual(
        self,
        connector_key: Tuple[str, str],
        cleanup_code: str,
        retained_for_key: Mapping[str, Any],
    ) -> CapabilityErrorDetail:
        """Publish a pre-commit residual without changing connector truth."""
        if retained_for_key:
            self._connector_cleanup_indices[connector_key] = dict(
                retained_for_key
            )
        detail = CapabilityErrorDetail(
            code="connector_cleanup_residual",
            message=(
                "Connector cleanup is incomplete for "
                f"'{self._index_key(*connector_key)}'"
            ),
            connector=connector_key[1],
            feature="connector_cleanup",
            reason="cleanup_incomplete",
            cleanup_scope="connector",
            cleanup_code=cleanup_code,
        )
        self._connector_creation_residuals[connector_key] = detail.model_dump(
            exclude_none=True
        )
        self._signal_market_state_change(connector_key)
        return detail

    def _commit_terminal_connector_stop(
        self,
        connector_key: Tuple[str, str],
        connector,
        stopped_indices: list[Tuple[str, Any]],
    ) -> None:
        """Synchronously publish terminal truth after physical network stop."""
        account_name, connector_name = connector_key
        cache_key = self._index_key(account_name, connector_name)
        if account_name == "__data__":
            if self._data_connectors.get(connector_name) is connector:
                self._data_connectors.pop(connector_name, None)
            self._data_connectors_started.pop(connector_name, None)
        else:
            account_connectors = self._trading_connectors.get(account_name)
            if (
                account_connectors is not None
                and account_connectors.get(connector_name) is connector
            ):
                account_connectors.pop(connector_name, None)
                if not account_connectors:
                    self._trading_connectors.pop(account_name, None)

        mappings = {
            "orders": self._orders_recorders,
            "funding": self._funding_recorders,
            "metrics": self._metrics_collectors,
        }
        for label, value in stopped_indices:
            mapping = mappings[label]
            if mapping.get(cache_key) is value:
                mapping.pop(cache_key, None)

        for registry in (
            self._connector_cleanup_owners,
            self._connector_residual_tasks,
            self._connector_cleanup_phases,
            self._market_cleanup_residuals,
            self._market_cleanup_owners,
            self._market_pair_owners,
            self._market_residual_tasks,
            self._market_cleanup_phases,
        ):
            for raw_key in tuple(registry):
                if (
                    isinstance(raw_key, tuple)
                    and raw_key[:2] == connector_key
                ):
                    registry.pop(raw_key, None)
        self._connector_creation_residuals.pop(connector_key, None)
        self._connector_cleanup_indices.pop(connector_key, None)
        self._connector_incarnations.pop(connector_key, None)
        try:
            setattr(connector, "_api_network_stop_terminal", True)
        except Exception:
            pass
        self._signal_market_state_change(connector_key)

    def _has_auxiliary_stop_state(
        self,
        connector_key: Tuple[str, str],
        network_owner_key: Tuple[str, str, str],
    ) -> bool:
        """Return whether terminal stop still has a distinct owner to reconcile."""
        for registry in (
            self._connector_cleanup_owners,
            self._connector_residual_tasks,
            self._connector_cleanup_phases,
            self._market_cleanup_residuals,
            self._market_cleanup_owners,
            self._market_pair_owners,
            self._market_residual_tasks,
            self._market_cleanup_phases,
        ):
            for raw_key in registry:
                if (
                    isinstance(raw_key, tuple)
                    and raw_key[:2] == connector_key
                    and raw_key != network_owner_key
                ):
                    return True
        return False

    async def await_retained_connector_stop_already_locked(
        self, account_name: str, connector_name: str
    ) -> Optional[bool]:
        """Wait for a retained physical stop while preserving account authority."""
        self._require_exact_account_root(account_name)
        connector_key = self._connector_key(account_name, connector_name)
        owner_key = self._owner_key(account_name, connector_name, "network")
        phase = self._connector_cleanup_phases.get(owner_key, {}).get(
            "connector_stop"
        )
        if not isinstance(phase, dict):
            return None
        task = phase.get("task")
        authority = phase.get("authority", _OWNER_AUTHORITY_UNSET)
        if not isinstance(task, asyncio.Task):
            return None
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                continue
        self._consume_background_task_result(task)
        if authority is _OWNER_AUTHORITY_UNSET:
            return phase.get("error_code") is None
        return self._cached_public_connector(connector_key) is not authority

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
        self._signal_market_state_change(connector_key)
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
        connector_key = self._connector_key(account_name, connector_name)
        lease = self._active_market_transaction_lease(connector_key)
        if lease is not None and not self._retain_market_transaction_descendant(
            connector_key, cleanup_task
        ):
            cleanup_task.cancel()
            await asyncio.gather(cleanup_task, return_exceptions=True)
            raise RuntimeError("connector_creation_cleanup_authority_unavailable")
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
        connector_name: str,
        *,
        api_keys: Optional[Dict[str, Any]] = None,
    ) -> ConnectorBase:
        """Create a trading connector with API keys.

        For Gateway network connectors (e.g., 'solana-mainnet-beta'), creates a unified
        Gateway connector which auto-detects chain/network and uses the default wallet.
        The dex_name and trading_type are passed to methods, not to the connector.
        """
        if api_keys is None:
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
        keys = (
            BackendAPISecurity.api_keys(connector_name)
            if api_keys is None
            else api_keys
        )

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
        """Start one freshly constructed connector's network tasks."""
        try:
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
        for attr_name in _TRACKER_TASK_CONTAINERS:
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
            for attr_name in _TRACKER_TASK_CONTAINERS:
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

    def _claim_connector_stop_owner(
        self,
        owner_key: Tuple[str, str, str],
        connector: ConnectorBase,
    ) -> None:
        """Publish one physical stop owner without rebinding retained truth."""
        retained_owner = self._connector_cleanup_owners.get(owner_key)
        if retained_owner is not None and retained_owner is not connector:
            raise RuntimeError("connector_stop_authority_mismatch")
        self._connector_cleanup_owners[owner_key] = connector

    async def _stop_connector_network(
        self,
        connector: ConnectorBase,
        account_name: str = "__unscoped__",
        connector_name: str = "__unscoped__",
        *,
        authority_incarnation=_OWNER_AUTHORITY_UNSET,
        owner_kind: str = "network",
        stopped_indices: Optional[list[Tuple[str, Any]]] = None,
        retained_indices: Optional[Mapping[str, Any]] = None,
        commit_all_on_success: bool = True,
    ):
        """Stop connector tasks with a hard deadline and retain pending handles."""
        self._ensure_connector_cleanup_state()
        if getattr(connector, "_api_network_stop_terminal", False):
            return
        owner_key = self._owner_key(account_name, connector_name, owner_kind)
        self._claim_connector_stop_owner(owner_key, connector)
        if authority_incarnation is _OWNER_AUTHORITY_UNSET:
            authority_incarnation = self._connector_incarnation(
                (account_name, connector_name)
            )
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

        # Connector lifecycle owns tracker stop/drain; the service invokes only
        # the connector boundary and drains the task handles it invalidates.
        if hasattr(connector, 'stop_network'):
            try:
                await self._run_owner_operation(
                    owner_key,
                    "connector_stop",
                    connector.stop_network,
                    authority=connector,
                    authority_incarnation=authority_incarnation,
                    terminal_tasks=tasks,
                    terminal_context={
                        "stopped_indices": tuple(stopped_indices or ()),
                        "retained_indices": dict(retained_indices or {}),
                        "commit_all_on_success": commit_all_on_success,
                    },
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
        *,
        collect_connector_tasks: bool = True,
        authority_incarnation=_OWNER_AUTHORITY_UNSET,
        stopped_indices: Optional[list[Tuple[str, Any]]] = None,
        retained_indices: Optional[Mapping[str, Any]] = None,
        commit_all_on_success: bool = True,
    ) -> None:
        """Stop one published data connector without double-stopping its tracker."""
        self._ensure_connector_cleanup_state()
        if getattr(connector, "_api_network_stop_terminal", False):
            return
        owner_key = self._owner_key("__data__", connector_name, "data_network")
        self._claim_connector_stop_owner(owner_key, connector)
        if authority_incarnation is _OWNER_AUTHORITY_UNSET:
            authority_incarnation = self._connector_incarnation(
                ("__data__", connector_name)
            )
        if not await self._drain_owner_tasks(owner_key):
            raise RuntimeError("task_cleanup_pending")

        tasks = (
            self._collect_connector_tasks(connector)
            if collect_connector_tasks
            else set()
        )
        stop_network = getattr(connector, "stop_network", None)
        if stop_network is None:
            raise RuntimeError("connector_stop_unavailable")
        await self._run_owner_operation(
            owner_key,
            "connector_stop",
            stop_network,
            authority=connector,
            authority_incarnation=authority_incarnation,
            terminal_tasks=tasks,
            terminal_context={
                "stopped_indices": tuple(stopped_indices or ()),
                "retained_indices": dict(retained_indices or {}),
                "commit_all_on_success": commit_all_on_success,
            },
        )
        if collect_connector_tasks:
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
        async with self.connector_state_authority(
            connector, connector_name, account_name
        ):
            await self._update_connector_state(
                connector, connector_name, account_name
            )

    @asynccontextmanager
    async def connector_state_authority(
        self,
        connector: ConnectorBase,
        connector_name: str,
        account_name: Optional[str] = None,
    ):
        """Keep one exact live connector stable through refresh, reads, and commit."""
        connector_key = self._connector_key(
            account_name or "__data__", connector_name
        )
        async with self._market_transaction_lock(connector_key):
            async with self._connector_creation_lock(connector_key):
                if self._cached_public_connector(connector_key) is not connector:
                    raise ConnectorCapabilityError(
                        CapabilityErrorDetail(
                            code="connector_incarnation_changed",
                            message=(
                                f"Connector '{connector_name}' changed while "
                                "refreshing state"
                            ),
                            connector=connector_name,
                            feature="connector_state",
                            reason="connector_incarnation_changed",
                        )
                    )
                yield

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
                tasks.append(
                    self.refresh_connector_state(
                        connector, connector_name, account_name
                    )
                )
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

    def _create_metrics_collector(
        self,
        connector: ConnectorBase,
        account_name: str,
        connector_name: str,
    ) -> Optional[TradeVolumeMetricCollector]:
        """Construct one collector without starting or publishing it."""
        if "_paper_trade" in connector_name:
            return None

        try:
            instance_id = f"{account_name}_hbotapi"
            rate_provider = self._rate_provider
            if rate_provider is None:
                logger.debug(f"No rate provider set, skipping trade-volume metrics for {connector_name}")
                return None

            metrics_collector = TradeVolumeMetricCollector(
                connector=connector,
                activation_interval=self.METRICS_ACTIVATION_INTERVAL,
                rate_provider=rate_provider,
                instance_id=instance_id,
                valuation_token=self.METRICS_VALUATION_TOKEN
            )
            return metrics_collector

        except Exception as e:
            logger.warning(f"Failed to init metrics for {connector_name}: {e}")
            return None

    def _initialize_metrics(
        self,
        connector: ConnectorBase,
        account_name: str,
        connector_name: str,
        cache_key: str,
        *,
        staged_indices: Optional[Dict[str, Any]] = None,
    ):
        """Initialize and publish a trade volume metrics collector."""
        if staged_indices is None and cache_key in self._metrics_collectors:
            return
        metrics_collector = self._create_metrics_collector(
            connector, account_name, connector_name
        )
        if metrics_collector is not None:
            if staged_indices is not None:
                staged_indices["metrics"] = metrics_collector
            try:
                metrics_collector.start()
            except Exception as error:
                if staged_indices is not None:
                    raise
                logger.warning(
                    f"Failed to start metrics for {connector_name}: {error}"
                )
                return
            if staged_indices is None:
                self._metrics_collectors[cache_key] = metrics_collector

    # =========================================================================
    # Credentials and Configuration
    # =========================================================================

    @asynccontextmanager
    async def account_transaction(self, account_name: str):
        """Serialize every connector-key publication for one whole account."""
        self._ensure_connector_cleanup_state()
        current_task = asyncio.current_task()
        if current_task is None:
            raise RuntimeError("account_transaction_task_unavailable")
        if self._account_transaction_owners.get(account_name) is current_task:
            yield
            return

        async with self._account_transaction_locks_guard:
            lock = self._account_transaction_locks.setdefault(
                account_name, asyncio.Lock()
            )
            self._account_transaction_lock_users[account_name] = (
                self._account_transaction_lock_users.get(account_name, 0) + 1
            )
        acquired = False
        try:
            await lock.acquire()
            acquired = True
            self._account_transaction_owners[account_name] = current_task
            yield
        finally:
            if self._account_transaction_owners.get(account_name) is current_task:
                self._account_transaction_owners.pop(account_name, None)
            if acquired and lock.locked():
                lock.release()
            # No await is allowed after releasing the physical account lock.
            # These registry mutations are one event-loop-atomic operation, so
            # repeated caller cancellation cannot strand a zero-owner entry.
            users = max(
                self._account_transaction_lock_users.get(account_name, 1) - 1,
                0,
            )
            if users:
                self._account_transaction_lock_users[account_name] = users
            else:
                self._account_transaction_lock_users.pop(account_name, None)
                if self._account_transaction_locks.get(account_name) is lock:
                    self._account_transaction_locks.pop(account_name, None)

    def _require_exact_account_root(self, account_name: str) -> asyncio.Task:
        current_task = asyncio.current_task()
        if (
            current_task is None
            or self._account_transaction_owners.get(account_name) is not current_task
        ):
            raise RuntimeError("account_transaction_authority_unavailable")
        return current_task

    @asynccontextmanager
    async def credential_transaction(
        self, account_name: str, connector_name: str
    ):
        """Hold account and exact connector authority across one credential commit."""
        connector_key = self._connector_key(account_name, connector_name)
        self._raise_market_mutation_preflight(connector_key)
        async with self.account_transaction(account_name):
            async with self._market_transaction_lock(connector_key) as token:
                yield token

    def _require_exact_market_root(
        self, connector_key: Tuple[str, str]
    ) -> _MarketTransactionLease:
        lease = self._exact_market_transaction_lease(connector_key)
        if lease is None:
            raise RuntimeError("market_transaction_authority_unavailable")
        return lease

    @staticmethod
    def _security_state_snapshot() -> Dict[str, Any]:
        secure_configs = getattr(BackendAPISecurity, "_secure_configs", None)
        decryption_done = getattr(BackendAPISecurity, "_decryption_done", None)
        return {
            "secure_configs": (
                copy.deepcopy(secure_configs)
                if secure_configs is not None
                else None
            ),
            "decryption_done": (
                decryption_done.is_set()
                if decryption_done is not None
                and hasattr(decryption_done, "is_set")
                else None
            ),
        }

    def _credential_publication_snapshot(
        self, account_name: str, connector_name: str
    ) -> Dict[str, Any]:
        credentials_path = (
            f"credentials/{account_name}/connectors/{connector_name}.yml"
        )
        existed = fs_util.path_exists(credentials_path)
        connector_settings = copy.deepcopy(
            AllConnectorSettings.get_connector_config_keys(connector_name)
        )
        return {
            "path": credentials_path,
            "existed": existed,
            "content": fs_util.read_file(credentials_path) if existed else None,
            "security": self._security_state_snapshot(),
            "connector_settings": connector_settings,
        }

    def _restore_staged_credential_publication(
        self,
        account_name: str,
        connector_name: str,
        stage: Mapping[str, Any],
    ) -> None:
        if stage.get("credentials_committed") or stage.get("credentials_rolled_back"):
            return
        snapshot = stage.get("credential_snapshot")
        if snapshot is None or not stage.get("credentials_touched"):
            return

        credentials_path = snapshot["path"]
        try:
            if snapshot["existed"]:
                fs_util.ensure_file_and_dump_text(
                    credentials_path, snapshot["content"]
                )
            elif fs_util.path_exists(credentials_path):
                fs_util.delete_file(
                    directory=f"credentials/{account_name}/connectors",
                    file_name=f"{connector_name}.yml",
                )
        finally:
            security = snapshot["security"]
            secure_configs = getattr(BackendAPISecurity, "_secure_configs", None)
            if secure_configs is not None and security["secure_configs"] is not None:
                secure_configs.clear()
                secure_configs.update(security["secure_configs"])
            decryption_done = getattr(BackendAPISecurity, "_decryption_done", None)
            if decryption_done is not None and security["decryption_done"] is not None:
                if security["decryption_done"]:
                    decryption_done.set()
                else:
                    decryption_done.clear()
            connector_settings = snapshot.get("connector_settings")
            update_settings = getattr(
                AllConnectorSettings, "update_connector_config_keys", None
            )
            if connector_settings is not None and callable(update_settings):
                update_settings(connector_settings)
            if isinstance(stage, dict):
                stage["credentials_rolled_back"] = True

    async def stage_connector_keys_already_locked(
        self,
        account_name: str,
        connector_name: str,
        keys: dict,
    ) -> Dict[str, Any]:
        """Validate a replacement connector without publishing file or cache state."""
        connector_key = self._connector_key(account_name, connector_name)
        self._require_exact_market_root(connector_key)
        self._require_exact_account_root(account_name)
        if not BackendAPISecurity.login_account(
            account_name=account_name,
            secrets_manager=self.secrets_manager,
        ):
            raise ValueError(f"Failed to authenticate for {account_name}")

        connector_config = HummingbotAPIConfigAdapter(
            copy.deepcopy(
                AllConnectorSettings.get_connector_config_keys(connector_name)
            )
        )
        for key, value in keys.items():
            setattr(connector_config, key, value)
        api_keys = api_keys_from_connector_config_map(connector_config)
        connector = self._create_trading_connector(
            account_name, connector_name, api_keys=api_keys
        )
        stage = {
            "connector": connector,
            "config": connector_config,
            "indices": {},
            "activated": False,
            "committed": False,
            "credential_snapshot": self._credential_publication_snapshot(
                account_name, connector_name
            ),
            "credentials_touched": False,
            "credentials_committed": False,
            "credentials_rolled_back": False,
        }
        try:
            await self._initialize_trading_connector_candidate(
                connector, account_name, connector_name
            )
            await self._activate_trading_connector_candidate(
                connector,
                account_name,
                connector_name,
                staged_indices=stage["indices"],
            )
            stage["activated"] = True
            stage["credentials_touched"] = True
            BackendAPISecurity.update_connector_keys(
                account_name, stage["config"]
            )
            BackendAPISecurity.decrypt_all(account_name=account_name)
        except BaseException:
            await self.discard_staged_connector_already_locked(
                account_name, connector_name, stage
            )
            raise
        return stage

    async def discard_staged_connector_already_locked(
        self,
        account_name: str,
        connector_name: str,
        stage: Mapping[str, Any],
    ) -> None:
        """Undo every private candidate delta and restore credential publication."""
        connector_key = self._connector_key(account_name, connector_name)
        self._require_exact_market_root(connector_key)
        self._require_exact_account_root(account_name)
        connector = stage.get("connector")
        if connector is None or stage.get("discarded"):
            return
        if isinstance(stage, dict):
            stage["discarded"] = True
        if self._cached_public_connector(connector_key) is connector:
            account_connectors = self._trading_connectors.get(account_name, {})
            account_connectors.pop(connector_name, None)
            if not account_connectors:
                self._trading_connectors.pop(account_name, None)
        cleanup_error = None
        for label, value in tuple(stage.get("indices", {}).items()):
            try:
                await self._stop_owned_index(
                    account_name,
                    connector_name,
                    f"credential_candidate_{label}",
                    value,
                )
            except BaseException as exc:
                cleanup_error = cleanup_error or exc
        try:
            await self._stop_connector_network(
                connector,
                account_name,
                connector_name,
                owner_kind="credential_candidate_network",
            )
        except BaseException as exc:
            cleanup_error = cleanup_error or exc
        try:
            self._restore_staged_credential_publication(
                account_name, connector_name, stage
            )
        except BaseException as exc:
            cleanup_error = cleanup_error or exc
        if cleanup_error is not None:
            raise cleanup_error

    async def commit_staged_connector_keys_already_locked(
        self,
        account_name: str,
        connector_name: str,
        stage: Dict[str, Any],
    ) -> ConnectorBase:
        """Stop the old generation, then publish only synchronous staged state."""
        connector_key = self._connector_key(account_name, connector_name)
        self._require_exact_market_root(connector_key)
        self._require_exact_account_root(account_name)
        connector = stage["connector"]
        detail = await self._stop_trading_connector_already_locked(
            account_name, connector_name
        )
        if detail is not None:
            raise ConnectorCapabilityError(detail)

        self._trading_connectors.setdefault(account_name, {})[
            connector_name
        ] = connector
        cache_key = self._index_key(account_name, connector_name)
        for label, mapping in (
            ("orders", self._orders_recorders),
            ("funding", self._funding_recorders),
            ("metrics", self._metrics_collectors),
        ):
            value = stage["indices"].get(label)
            if value is not None:
                mapping[cache_key] = value
        self._advance_connector_incarnation(connector_key)
        self._connector_creation_residuals.pop(connector_key, None)
        stage["credentials_committed"] = True
        stage["committed"] = True
        self._signal_market_state_change(connector_key)
        return connector

    async def update_connector_keys(
        self,
        account_name: str,
        connector_name: str,
        keys: dict
    ) -> ConnectorBase:
        """Update API keys and recreate connector."""
        async with self.credential_transaction(account_name, connector_name):
            stage = await self.stage_connector_keys_already_locked(
                account_name, connector_name, keys
            )
            try:
                return await self.commit_staged_connector_keys_already_locked(
                    account_name, connector_name, stage
                )
            except BaseException:
                if not stage.get("committed"):
                    await self.discard_staged_connector_already_locked(
                        account_name, connector_name, stage
                    )
                raise

    def clear_trading_connector_already_locked(
        self, account_name: str, connector_name: str
    ) -> None:
        """Clear exact cache state only for the current transaction root."""
        connector_key = self._connector_key(account_name, connector_name)
        self._require_exact_market_root(connector_key)
        account_connectors = self._trading_connectors.get(account_name)
        if account_connectors is not None:
            account_connectors.pop(connector_name, None)
            if not account_connectors:
                self._trading_connectors.pop(account_name, None)

    def clear_account_connectors_already_locked(self, account_name: str) -> None:
        """Clear one account only for the exact account-transaction root."""
        self._require_exact_account_root(account_name)
        self._trading_connectors.pop(account_name, None)

    def clear_trading_connector(
        self,
        account_name: Optional[str] = None,
        connector_name: Optional[str] = None
    ):
        """Clear terminal cache state; live physical owners reject the mutation."""
        self._ensure_connector_cleanup_state()
        if account_name is not None:
            owner = self._account_transaction_owners.get(account_name)
            try:
                current_task = asyncio.current_task()
            except RuntimeError:
                current_task = None
            if owner is not None and owner is not current_task:
                raise RuntimeError("account_transaction_active")
        keys = []
        if account_name and connector_name:
            keys.append((account_name, connector_name))
        elif account_name:
            keys.extend(
                (account_name, name)
                for name in self._trading_connectors.get(account_name, {})
            )
        else:
            keys.extend(
                (owner_account, name)
                for owner_account, connectors in self._trading_connectors.items()
                for name in connectors
            )
        for connector_key in keys:
            self._raise_market_mutation_preflight(connector_key)
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
        connector_key = self._connector_key(account_name, connector_name)
        self._ensure_connector_cleanup_state()
        exact_lease = self._exact_market_transaction_lease(connector_key)
        if exact_lease is not None:
            owner = self._account_transaction_owners.get(account_name)
            if owner is not None and owner is not asyncio.current_task():
                raise RuntimeError("account_transaction_active")
            detail = await self._stop_trading_connector_already_locked(
                account_name, connector_name
            )
            if detail is not None:
                raise ConnectorCapabilityError(detail)
            return True
        if self._active_market_transaction_lease(connector_key) is not None:
            raise RuntimeError("market_transaction_authority_unavailable")
        self._raise_market_mutation_preflight(connector_key)
        async with self.account_transaction(account_name):
            detail = await self._locked_stop_trading_connector(
                account_name, connector_name
            )
        if detail is not None:
            raise ConnectorCapabilityError(detail)
        return True

    @staticmethod
    def _consume_background_task_result(task: asyncio.Task) -> None:
        try:
            task.exception()
        except (asyncio.CancelledError, Exception):
            pass

    async def _locked_stop_trading_connector(
        self, account_name: str, connector_name: str
    ) -> Optional[CapabilityErrorDetail]:
        """Acquire market authority before connector authority for every stop."""
        connector_key = self._connector_key(account_name, connector_name)
        async with self._market_transaction_lock(connector_key):
            return await self._stop_trading_connector_already_locked(
                account_name, connector_name
            )

    async def _stop_trading_connector_already_locked(
        self, account_name: str, connector_name: str
    ) -> Optional[CapabilityErrorDetail]:
        """Stop under an existing market lease without public-lock reentry."""
        connector_key = self._connector_key(account_name, connector_name)
        async with self._connector_creation_lock(connector_key):
            return await self._finalize_stop_trading_connector(
                account_name, connector_name
            )

    async def _finalize_stop_data_connector(
        self,
        connector_name: str,
        *,
        invoking_owner_key: Optional[Tuple[str, str, str]] = None,
    ) -> Optional[CapabilityErrorDetail]:
        """Finalize a data generation while reconciling every same-key owner."""
        return await self._finalize_stop_trading_connector(
            "__data__",
            connector_name,
            invoking_owner_key=invoking_owner_key,
        )

    async def _finalize_stop_trading_connector(
        self,
        account_name: str,
        connector_name: str,
        *,
        invoking_owner_key: Optional[Tuple[str, str, str]] = None,
    ) -> Optional[CapabilityErrorDetail]:
        """Run bounded connector cleanup and retain failed indices for an explicit retry."""
        self._ensure_connector_cleanup_state()
        connector_key = self._connector_key(account_name, connector_name)
        cache_key = self._index_key(account_name, connector_name)
        cleanup_codes = []
        stopped_indices = []
        retained_indices = self._connector_cleanup_indices
        retained_for_key = retained_indices.get(connector_key, {})
        network_owner_key = self._owner_key(
            account_name,
            connector_name,
            "data_network" if account_name == "__data__" else "network",
        )
        network_phase = self._connector_cleanup_phases.get(
            network_owner_key, {}
        ).get("connector_stop")
        indices = (
            ("orders", self._orders_recorders),
            ("funding", self._funding_recorders),
            ("metrics", self._metrics_collectors),
        )
        if isinstance(network_phase, dict) and "stopped_indices" in network_phase:
            stopped_indices = list(network_phase["stopped_indices"])
            retained_for_key = dict(
                network_phase.get("retained_indices", retained_for_key)
            )
        else:
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
                except BaseException as exc:
                    if isinstance(exc, asyncio.CancelledError):
                        rollback_code = (
                            await self._resume_pre_stop_indices_before_raise(
                                connector_key, stopped_indices
                            )
                        )
                        if rollback_code is not None:
                            self._retain_connector_cleanup_residual(
                                connector_key,
                                rollback_code,
                                retained_for_key,
                            )
                        raise
                    cleanup_codes.append(f"{label}_cleanup_incomplete")
                    retained_for_key[label] = value
                else:
                    retained_for_key.pop(label, None)
                    stopped_indices.append((label, value))

        if cleanup_codes:
            rollback_code = await self._resume_pre_stop_indices(
                connector_key, stopped_indices
            )
            return self._retain_connector_cleanup_residual(
                connector_key,
                rollback_code or cleanup_codes[0],
                retained_for_key,
            )

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
        try:
            for owner_key in sorted(restart_owner_keys):
                if await self._verify_restart_owner_phases(owner_key):
                    verified_restart_owners.add(owner_key)
                else:
                    cleanup_codes.append("restart_cleanup_incomplete")
        except BaseException as exc:
            if isinstance(exc, asyncio.CancelledError):
                rollback_code = await self._resume_pre_stop_indices_before_raise(
                    connector_key, stopped_indices
                )
                if rollback_code is not None:
                    self._retain_connector_cleanup_residual(
                        connector_key, rollback_code, retained_for_key
                    )
            raise

        restart_phase_failed = bool(
            restart_owner_keys - verified_restart_owners
        )
        if restart_phase_failed:
            rollback_code = await self._resume_pre_stop_indices(
                connector_key, stopped_indices
            )
            return self._retain_connector_cleanup_residual(
                connector_key,
                rollback_code or cleanup_codes[0],
                retained_for_key,
            )
        account_connectors = self._trading_connectors.get(account_name)
        data_connector = (
            self._data_connectors.get(connector_name)
            if account_name == "__data__"
            else None
        )
        retained_network_owner = self._connector_cleanup_owners.get(
            network_owner_key
        )
        data_owner = (
            retained_network_owner if account_name == "__data__" else None
        )
        connector = retained_network_owner
        if connector is None:
            connector = data_connector
        if connector is None and account_connectors:
            connector = account_connectors.get(connector_name)
        stopped_connectors = []
        network_cleanup_failed = restart_phase_failed
        if connector is not None:
            try:
                if account_name == "__data__" and (
                    connector is data_connector or connector is data_owner
                ):
                    await self._stop_data_connector_network(
                        connector,
                        connector_name,
                        collect_connector_tasks=invoking_owner_key is None,
                        stopped_indices=stopped_indices,
                        retained_indices=retained_for_key,
                        commit_all_on_success=invoking_owner_key is None,
                    )
                else:
                    await self._stop_connector_network(
                        connector,
                        account_name,
                        connector_name,
                        stopped_indices=stopped_indices,
                        retained_indices=retained_for_key,
                        commit_all_on_success=invoking_owner_key is None,
                    )
            except asyncio.CancelledError:
                if (
                    getattr(connector, "_api_network_stop_terminal", False)
                    and invoking_owner_key is None
                    and not self._has_auxiliary_stop_state(
                        connector_key, network_owner_key
                    )
                ):
                    self._commit_terminal_connector_stop(
                        connector_key, connector, stopped_indices
                    )
                    return None
                network_phase = self._connector_cleanup_phases.get(
                    network_owner_key, {}
                ).get("connector_stop")
                if not isinstance(network_phase, dict):
                    rollback_code = (
                        await self._resume_pre_stop_indices_before_raise(
                            connector_key, stopped_indices
                        )
                    )
                    if rollback_code is not None:
                        self._retain_connector_cleanup_residual(
                            connector_key, rollback_code, retained_for_key
                        )
                raise
            except BaseException:
                terminal_committed = getattr(
                    connector, "_api_network_stop_terminal", False
                )
                if (
                    terminal_committed
                    and invoking_owner_key is None
                    and not self._has_auxiliary_stop_state(
                        connector_key, network_owner_key
                    )
                ):
                    self._commit_terminal_connector_stop(
                        connector_key, connector, stopped_indices
                    )
                    logger.info(
                        "Stopped trading connector %s/%s",
                        account_name,
                        connector_name,
                    )
                    return None
                network_phase = self._connector_cleanup_phases.get(
                    network_owner_key, {}
                ).get("connector_stop")
                if isinstance(network_phase, dict):
                    if not network_phase.get("complete"):
                        cleanup_code = "connector_stop_pending"
                    elif network_phase.get("pre_stop_reconciled"):
                        cleanup_code = (
                            network_phase.get("rollback_code")
                            or network_phase.get("error_code")
                            or "connector_cleanup_incomplete"
                        )
                    else:
                        rollback_code = await self._resume_pre_stop_indices(
                            connector_key, stopped_indices
                        )
                        cleanup_code = (
                            rollback_code or "connector_cleanup_incomplete"
                        )
                else:
                    rollback_code = await self._resume_pre_stop_indices(
                        connector_key, stopped_indices
                    )
                    cleanup_code = rollback_code or "connector_cleanup_incomplete"
                return self._retain_connector_cleanup_residual(
                    connector_key,
                    cleanup_code,
                    retained_for_key,
                )
            else:
                if (
                    invoking_owner_key is None
                    and not self._has_auxiliary_stop_state(
                        connector_key, network_owner_key
                    )
                ):
                    self._commit_terminal_connector_stop(
                        connector_key, connector, stopped_indices
                    )
                    logger.info(
                        "Stopped trading connector %s/%s",
                        account_name,
                        connector_name,
                    )
                    return None
                stopped_connectors.append(connector)
                if account_name == "__data__" and (
                    connector is data_connector or connector is data_owner
                ):
                    if self._data_connectors.get(connector_name) is connector:
                        self._data_connectors.pop(connector_name, None)
                    self._data_connectors_started.pop(connector_name, None)
                elif (
                    account_connectors
                    and account_connectors.get(connector_name) is connector
                ):
                    account_connectors.pop(connector_name, None)
                if account_connectors is not None and not account_connectors:
                    self._trading_connectors.pop(account_name, None)

        # The invoking public transaction is part of this stop, not a residual
        # that should block it. Commit its disposition as soon as its exact
        # connector has stopped and been unpublished; other owners remain below.
        expected_public_owner = self._owner_key(
            account_name, connector_name, "public_transaction"
        )
        if (
            invoking_owner_key == expected_public_owner
            and connector is not None
            and any(connector is stopped for stopped in stopped_connectors)
        ):
            invoking_owner = self._connector_cleanup_owners.get(
                invoking_owner_key
            )
            invoking_phase = self._connector_cleanup_phases.get(
                invoking_owner_key, {}
            ).get("disposition")
            if (
                isinstance(invoking_owner, dict)
                and invoking_owner.get("connector") is connector
                and invoking_owner.get("disposition") == "discard"
                and invoking_phase is not None
            ):
                invoking_phase["complete"] = True
                invoking_phase["result"] = "discard"
                invoking_phase["error_code"] = None
                self._finish_owner_cleanup(invoking_owner_key)

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
            market_incarnation = (
                owner.get("incarnation") if isinstance(owner, dict) else None
            )
            if (
                market_connector is not None
                and not any(
                    market_connector is stopped for stopped in stopped_connectors
                )
            ):
                if restart_phase_failed or network_cleanup_failed:
                    continue
                try:
                    await self._stop_connector_network(
                        market_connector,
                        account_name,
                        connector_name,
                        authority_incarnation=market_incarnation,
                    )
                except asyncio.CancelledError:
                    raise
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
            if owner_key == invoking_owner_key:
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

        cleanup_codes.extend(
            self._reconcile_orphan_connector_phases(connector_key)
        )

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
            mappings = {
                "orders": self._orders_recorders,
                "funding": self._funding_recorders,
                "metrics": self._metrics_collectors,
            }
            for label, value in stopped_indices:
                mapping = mappings[label]
                if mapping.get(cache_key) is value:
                    mapping.pop(cache_key, None)
            residuals.pop(connector_key, None)
            retained_indices.pop(connector_key, None)
            for key in list(market_residuals):
                if key[:2] == connector_key:
                    market_residuals.pop(key, None)
                    market_owners.pop(key, None)
            logger.info(f"Stopped trading connector {account_name}/{connector_name}")
            self._prune_connector_incarnation(connector_key)
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
        self._signal_market_state_change(connector_key)
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
            self._connector_incarnations,
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
        for connector_key in tuple(self._connector_incarnations):
            self._prune_connector_incarnation(connector_key)
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
            connector_key = (owner_account, connector_name)
            async with self._market_transaction_lock(connector_key):
                async with self._connector_creation_lock(connector_key):
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
