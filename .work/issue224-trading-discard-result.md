# Issue #224 trading discard closure result

## Outcome

PASS. API-B1 from `review-issue224-final-6a40d14/.work/spec-review.md` is
closed, and the stale `ContextVar` snapshot advisory is repaired. Work started
from `6a40d1424342aab842e89e04426ff8040cda8556` on
`fix/issue224-manzana-bootstrap`. No push was performed.

## Repair

- Trading unsafe-discard passes its exact
  `(account, connector, public_transaction)` owner into connector finalization.
  Once that connector has stopped and been unpublished, its disposition phase
  is completed and its owner/phase/task ledgers are consumed before residual
  accounting. The successful stop is therefore not replayed on retry.
- The invoking public owner is the only owner consumed by that commit. A
  separate private owner remains retained and still produces
  `connector_cleanup_residual / owner_cleanup_incomplete`.
- `_PUBLIC_TRANSACTION_CONTEXT` now carries only a slotted token with `id` and
  `active`. Each service owns the active token-to-snapshot map used by the task
  factory. The boundary revokes the token and removes its snapshot mapping
  before resetting the context.
- Task creation ignores inactive or unmapped inherited tokens. A long-lived
  child from an earlier boundary can neither retain its old snapshot nor append
  a task to that snapshot while a later transaction has installed the factory.

## Strict TDD evidence

Three regressions were added before production changes. At the rejected head,
all three failed in normal Python and all three failed under `python -O`:

- unsafe trading discard falsely returned `owner_cleanup_incomplete` after one
  physical stop;
- a real separate private residual retained the already-completed public owner;
- a long-lived child inherited the full mutable snapshot instead of a revocable
  lightweight token.

On the final tree the focused Issue #224 selection passed with **20 passed** in
normal mode and **20 passed** in optimized mode. The three new adverse tests
also passed independently in both modes.

## Final verification

All pytest runs used CPython 3.13.5, disabled the cache plugin and bytecode
writes, and used a fixed hash seed. Optimized runs used `python -O -B`.

| Gate | Normal | Optimized (`-O`) |
| --- | ---: | ---: |
| New trading/token regressions | 3 passed | 3 passed |
| Focused Issue #224 selection | 20 passed | 20 passed |
| Complete Issue #222/#224 real-core module | 175 passed, 0 skipped | 175 passed, 0 skipped |
| Real-core B9 integration | 3 passed, 0 skipped | 3 passed, 0 skipped |
| ASGI/OpenAPI/hash/candle/error-contract slice | 14 passed | 14 passed |

Additional gates:

- Full accepted-core API collection: **1001 tests collected**, exit 0.
- Real-core root:
  `D:/HermesStaging/workspace/issue8-manzana-shift-rebuild/.work/accepted-core-build`.
- `git diff --check`: passed.
- Fatal `flake8` selection `E9,F63,F7,F82`: passed for both changed Python
  files using cached `flake8 7.5.0` in offline mode.
- AST parse and in-memory compile at optimization levels 0 and 2: passed for
  both changed Python files.
- Added-code checks: no bare asserts, no bare `except`, and no line longer than
  130 characters.

Warnings were existing pytest, SQLAlchemy, Pydantic, and dependency-stack
deprecations. No failure or skip was hidden.

## Changed scope

- `services/unified_connector_service.py`
- `test/test_issue222_manzana_capabilities.py`
- `.work/issue224-trading-discard-result.md`
