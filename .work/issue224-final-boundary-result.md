# Issue #224 final declaration-boundary result

## Status

**DONE.** The rejected head `e3c6f767cd30caad3b0973f9ae8101052c6637ab`
was repaired on branch `fix/issue224-manzana-bootstrap`. The sole blocker from
`review-issue224-final-e3c6f76/.work/spec-review.md` is closed.

## Root cause and repair

`UnifiedConnectorService` validated a trading connector's constructor declaration,
then allowed `_initialize_trading_pair_symbol_map()` to replace
`api_capabilities` without another validation before return and cache publication.
The public-data path had the same ordering gap after lazy symbol discovery and
before pair/tracker mutation.

The repair:

- validates the loaded declaration immediately after trading symbol-map initialization;
- validates once more at the final pre-return/pre-publication boundary;
- validates after public symbol discovery and before pair, book, or tracker mutation;
- shares a clean typed creation failure with callers already waiting on the same
  creation lock, preventing a second construction/stop of the failed connector;
- prunes the transient failure, connector lock, and lock-user state after the final
  waiter leaves so a later independent call may retry;
- keeps the existing bounded creation cleanup responsible for connector/tracker,
  recorder, funding-recorder, metric, task, cache, and ownership rollback.

## TDD evidence

The new boundary matrix was run before production edits in normal and optimized
mode. Both runs produced the intended red result: **4 failed, 3 passed**. The
failures showed both invalid final declarations were returned/published and both
public discovery cases proceeded without a typed rejection.

After the repair:

- bootstrap feature loss after symbol-map initialization: typed
  `connector_capability_mismatch` for both concurrent waiters;
- `order_submission` with non-callable `cancel`: the same typed failure for both
  concurrent waiters;
- each failed creation constructs once, stops the tracker once, stops the connector
  once, publishes nothing, and leaves no connector/recorder/metric/cleanup/lock residue;
- compatible pair-narrowed and complete `order_submission` supersets publish;
- a late declaration replacement after recorder/metric initialization is rejected at
  the final boundary and all created indices are stopped and removed;
- public symbol discovery rejects incompatible loaded truth before pair/tracker/book
  mutation and admits compatible pair-narrowed truth.

## Final validation gates

All Python verification used CPython 3.13.15, `-B`, disabled pytest's cache
provider, isolated environment variables, and fixed hash seeds. Optimized runs used
`-O -B`.

| Gate | Normal | Optimized |
|---|---:|---:|
| Focused Issue #224 including final-boundary regressions | 17 passed | 17 passed |
| Complete Issue #222 | 101 passed, 2 skipped | 101 passed, 2 skipped |
| Real accepted-core B9 | 2 passed | 2 passed |
| Dependency-light | 56 passed, 1 skipped, 1 deselected | 56 passed, 1 skipped, 1 deselected |
| ASGI / OpenAPI / deterministic hash | 9 passed | 9 passed |

Accepted-core full collection: **926 tests collected**, zero collection errors.

Static checks:

- `git diff --check`: clean;
- both changed Python files parse successfully with `ast`;
- zero `assert` statements, bare `except` handlers, or `eval`/`exec` calls in the
  changed Python files;
- zero added-production conflict markers, TODO/FIXME markers, shell execution,
  unsafe YAML/pickle, subprocess, or dynamic evaluation hits.

The first B9 setup attempts stopped during import because reusable environments
lacked harness-only dependencies (`hexbytes`, then accepted-core transitive
packages). They did not execute product tests. The final B9 runs used only local
offline caches and the pinned accepted core at
`0529e34eba03408311526089e617af7af75a9c2f` and passed in both modes.

## Cleanup and scope

- The disposable workspace-local verification environment was removed after use.
- The final workspace audit found zero `__pycache__`, `.pytest_cache`, `.pyc`, or
  `.pyo` artifacts.
- Pre-existing untracked `.work` prompts and receipts were preserved and are not
  part of the repair commit.
- No remote state was changed and no push was performed.

The repair follows the investigate workflow: root cause and mutation boundaries
were traced first, regressions were proven red, the minimal lifecycle change was
implemented, and every requested preservation gate was rerun.
