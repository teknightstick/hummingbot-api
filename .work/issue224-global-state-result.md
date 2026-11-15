# Issue #224 in-flight index and global-state closure

## Verdict

PASS. The five blockers reported against rejected API head
`5c3c61d2ce8d0dfb121ea0db80f022e1ef4cbd11` are closed. The fix is committed
in the commit containing this report; nothing was pushed.

Accepted core was exact head `a08babc04cb976c442305f3fae652f06f0526311`
(tree `f16f9e9c46321dd0684f5e362c20a12225f36dfc`). A clean Git archive produced
1,863 tracked files, and all 60 rebuilt CPython 3.13 extensions imported from
that disposable root with zero outside origins.

## Closure

1. An in-flight auxiliary index stop now carries the complete rollback prefix
   before its await. Cancellation retains the current index operation and its
   terminal continuation resumes the exact stopped prefix before cleanup can be
   considered reconciled.
2. Connector-stop phase state retains `unresolved_resume_indices`.
   `pre_stop_reconciled` becomes true only after the subset is empty; retries
   replay only failed resumes and never replay a successful resume or network
   stop.
3. Credential creation initializes staging state before awaits and distinguishes
   an absent prior connector from a successful retained stop. A canceled
   first-time creation discards the candidate, credential file/security stage,
   and account-state candidate instead of publishing partial truth.
4. One typed account-name validator now protects market-data request models,
   query routes, service entry points, and unified public account resolution.
   Every explicit `__*` account is rejected before connector lookup. Internal
   data shutdown now uses a dedicated private data-stop transaction rather than
   passing `__data__` through the public trading-account API.
5. Process-global `BackendAPISecurity` snapshot/mutation/commit/rollback is
   serialized by global transaction authority. Documented acquisition order is
   security, account, market, connector; a rollback cannot overwrite another
   account's later committed security state.

## Regression probes

Five exact probes were added for current-index cancellation, unresolved-only
resume retry, first-credential cancellation, reserved-account boundaries, and
cross-account security success/rollback. On the rejected implementation the
five-probe run failed 5/5. The repaired implementation passes them in both the
normal and optimized authoritative suites.

## Verification

| Gate | Normal | Optimized (`-O`) |
| --- | ---: | ---: |
| Exact accepted-core API + B9 modules | 249 passed, 0 skipped | 249 passed, 0 skipped |
| Standalone real-core B9 | 3 passed | 3 passed |
| Contract/error/hash/reserved-name slice | 38 passed | 38 passed |
| `TestGatewayWalletState` | 2 passed | 2 passed |

- Whole-repository collection: 1,072 tests collected, zero collection errors.
- Static compilation: 7/7 changed Python files in normal mode and 7/7 in
  optimized mode.
- Fatal-code flake8 (`E9,F63,F7,F82`): passed.
- `git diff --check`: passed.
- Added Python lines over 130 characters: zero.
- Accepted-core build/import: 60/60 extensions imported from the exact clean
  root.

Known output is limited to existing deprecation and pytest `-O` warnings; there
are no test failures, skips in the required suites, collection errors, or static
gate failures.
