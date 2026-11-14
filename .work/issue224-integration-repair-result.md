# Issue #224 consolidated API + exact-core integration repair

## Result

PASS. All six specification-review blockers and both exact-core integration blockers are repaired on rejected API head `0ca6cfbfe10e56daf408dc65e25a18d38247c883`, against accepted core `b8eacf25baec179d2ce46d604b6f8195854a0e6d`.

No external state-changing, GitHub, Docker, UAT, venue, order, or push action was performed. An early local `git cat-file` probe of the absent accepted-core object triggered an automatic promisor fetch attempt; it failed without changing local or remote state, and all subsequent core inspection used the supplied exact-core checkout.

## Root cause

The rejected transaction implementation restored whole mutable snapshots and inferred task ownership from the final reachable tracker state. That could erase unrelated concurrent state, lose the only handle to a replaced or cancellation-resistant task, and attempt to assign accepted core's read-only `order_books` property. Validation and public error translation also occurred too late or exposed collaborator exception text.

## Repairs

- Rollback now records and reverses only transaction-owned pair, book, task, started, and cache deltas. Pair/book restoration is keywise and identity/count guarded; unrelated concurrent mutations survive.
- Uninstrumented collaborator mutation is treated as unsafe. The connector is stopped and unpublished while residual state is retained instead of rewinding shared state.
- Every transaction-created task is captured at its creation/discovery boundary. Replaced task A remains owned; unrelated replacement B is not cancelled.
- Cancellation-resistant task handles remain indexed by residual, owner, and phase until the exact handle drains. Pending cleanup blocks terminal success, and retry awaits that handle without losing its sole reference.
- Pair-registration false, exception, and cancellation paths share final validation and owned-delta cleanup. Exceptions become stable typed `connector_pair_registration_failed` failures without collaborator text.
- Loaded Manzana declarations are validated before the no-tracker success path. Invalid cached/started connectors are stopped and unpublished or retained as residual-owned.
- `ensure_data_connector_started` creates its snapshot and ownership guard before preloaded validation, so typed mismatches clean invalid cache and started truth.
- The add-pair route documents typed HTTP 500 responses and emits the same stable `PublicErrorResponse` shape for false and exception paths.
- Accepted core tracker `order_books` is restored in place. No assignment is made to its read-only property; writable-field compatibility is preserved.
- Task inventory includes accepted core `_in_flight_transactions` and all other semantic task containers.
- Existing cancellation argument fidelity, data-network retry, monotonic/pruned incarnations, serialization, OpenAPI 400, callable validation, and no-direct-tracker-stop behavior remain covered.

## TDD evidence

- New blocker regressions first failed on the rejected implementation: 8 failures in normal mode and the same 8 failures under `python -O`.
- Focused blocker suites then passed in both modes.
- Full Issue #222/#224 module: `165 passed` normal; `165 passed` optimized.
- Exact accepted-core B9/real integration suite: `3 passed` normal; `3 passed` optimized; zero skips.
- ASGI/OpenAPI/hash selection: `9 passed` normal; `9 passed` optimized.
- Full dependency-light collection with exact core: `991 tests collected`; exit 0.
- Static gates: `git diff --check`, AST parse, and optimized `py_compile` passed for all four changed Python files.

The accepted core extensions were built locally from the exact checked-out SHA. Collection and real-core execution used narrow in-process shims only for unrelated optional gateway dependencies absent from the available environments (`aioprocessing`, `base58`, and gateway certificate helpers); the API service and Hummingbot tracker/exchange modules were the real repository/core implementations. Test warnings were existing deprecation and pytest optimized-mode warnings.

## Changed scope

- `services/unified_connector_service.py`
- `routers/market_data.py`
- `test/test_issue222_manzana_capabilities.py`
- `test/test_issue222_real_core_integration.py`
- This result receipt

Generated exact-core build products and Python caches were removed after verification. Pre-existing untracked `.work` review artifacts were left untouched.
