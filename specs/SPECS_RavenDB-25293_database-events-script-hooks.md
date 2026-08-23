# SPECS RavenDB-25293 — Database events script hooks (delete-only)

- **Issue:** RavenDB-25293 (http://issues.ravendb.net/issue/RavenDB-25293)
- **Type:** feature (tracked as Bug / Priority High / State Submitted / Target Release Backlog)
- **Status:** Implemented — see *Implementation status* below
- **Date:** 2026-08-23
- **Author:** specs-builder-generic (grounded by code analysis)
- **Base:** `ravendb/v6.2` @ `03e4c3b2ff1`; existing work on `copilot/ravendb-25293-add-database-events-hooks` (`603d71485b1`, `f157862294b`)

## Context / Goal

From the issue, verbatim:

> Please add configuration for running a script upon:
> - database deletion
> - database creation
>
> The script needs to pass database name(s) in question. Need to consider shell encoding/escape issues - maybe supplying base64 and regular string would do. Unless database name character set is safe for shell CLI args passing.
>
> The cloud needs to schedule a command to fulfill retention commitments - it would be implemented by sending a request to the cloud API upon database deletion.

The driving problem is RavenDB-22546: RavenDB Cloud applies a 2-week retention policy to user backups, triggered on backup or on product removal. When a user deletes a *database* (not the product), the backups of that database are never revisited and remain in cloud storage indefinitely, incurring charges. ppekrol's Nov 2025 comment on RavenDB-22546 settled the design — the backups have a different owner than the database, so a server-side retention timer is the wrong tool and the hooks described here are the chosen mechanism. The cost leak has recurred with customers (Zoho Desk 894323000036732269, Dec 2025).

## Scope decisions taken for this spec

Three forks were decided by the issue owner on 2026-08-23:

1. **Delete-only.** The `database creation` hook requested in the issue is **dropped**. This is a deliberate reduction against the issue text — the cloud retention use case needs deletion only, and the create hook as currently written is unsound (see *Why the create hook is dropped*).
2. **Fire at deletion initiation**, not at completion. Accepted with eyes open; the failure window is recorded under Cross-cutting concerns.
3. **Retry up to 3 attempts with exponential backoff** on script failure, rather than durable persisted delivery or pure best-effort.

## Confidence summary

Overall: **Medium-High**

| Section | Confidence | Basis |
|---|---|---|
| Requirements | High | stated in issue + owner decisions 2026-08-23 |
| Behavior & approach (delete path, config, exec) | High | code-verified, independently re-verified under review |
| Behavior & approach (retry design) | Medium | new work; the first draft's design was materially wrong and has been rewritten — the corrected shape is reasoned from code but unimplemented |
| Dependencies (cloud-side consumer) | Low | no RDBCL ticket exists; the consuming script and cloud API endpoint are unspecified |
| Data/config | High | code-verified |
| Security | Medium-High | code-verified; the first draft's central claim about scope enforcement was **wrong** and is corrected below |
| Cross-cutting | Medium-High | code-verified; residual gaps stated explicitly |

## Implementation status

All fourteen requirements are implemented on `copilot/ravendb-25293-add-database-events-hooks`. Full solution build clean (0 errors); the hook test suite passes.

Files changed: `DatabaseConfiguration.cs`, `DatabasesLandlord.cs`, `DatabaseExecUtils.cs` (rewritten), `AlertType.cs`, `ExtensionPointsTests.cs`, plus new `DatabaseEventExecTests.cs` and `DatabaseEventExecClusterTests.cs`.

Notes where the implementation resolved something the spec left open:

- **The `--` sentinel behaves differently per interpreter, and both were verified empirically rather than assumed.** `bash` passes `--` through to the script (so the database name is `$3`), while PowerShell's parameter binder consumes it (so the name binds to the second parameter). In both, a name like `--force` arrives as a *value*, which is the whole point of the sentinel. The tests strip a leading `--` line to stay cross-platform.
- **Attempt shape** (Open Question 2): per-attempt cap 30s, total budget 90s (new key), decorrelated jitter with 1s base and 8s cap, 3 attempts. A start failure that cannot resolve on retry (`ENOENT`, access denied, bad exe format) is attempted **once** — the inner `Win32Exception` is now preserved so that distinction survives.
- **Async attempt** (Open Question 5): took `Process.WaitForExitAsync` / `ReadToEndAsync(token)` rather than the `PoolOfThreads` route. Both are new to this repo but remove the blocking entirely, so one `CancellationTokenSource` per attempt now covers the process wait *and* both stream reads — which is what makes the configured timeout mean what it says.
- **Unknown deletion kind:** if `changeState` is not a `DeleteDatabaseCommand` the hook assumes a **hard** delete and logs at `Info`. In practice the cast always succeeds; this is defensive only.

Deliberately **not** done, matching the spec's own out-of-scope and non-blocking items: the shared-helper extraction with `DirectoryExecUtils` (Open Question 6), the put-settings scope check (Open Question 4), and anything cloud-side.

Not covered by tests: the "fails twice then succeeds" retry variant (the always-fails path is asserted at exactly 3 attempts), and an explicit assertion on the total retry budget.

## Requirements

Statuses below describe the state **before** this implementation; see *Implementation status* above.

| # | Requirement | Status | Where it lives |
|---|---|---|---|
| R1 | A server-wide configuration key names an executable to run when a database is deleted, with companion arguments and timeout keys | **Clear** — already implemented | `DatabaseConfiguration` |
| R2 | The database name is passed both plain-escaped and base64, so quoting cannot corrupt it | **Clear** — already implemented | `DatabaseExecUtils.ExecuteOnDatabaseEvent` |
| R3 | The hook fires once per cluster-wide database deletion, not once per node or per shard | **Clear** — already implemented | `DatabasesLandlord.HandleClusterDatabaseChanged` |
| R4 | The hook never blocks, delays, or vetoes the deletion — **hard invariant**, see below | **Clear** — already implemented | `DatabasesLandlord.RunDatabaseDeleteExec` |
| R5 | The create hook and its three config keys are removed | **New** | `DatabaseConfiguration`, `DatabasesLandlord`, `ExtensionPointsTests` |
| R6 | A failing script is retried up to 3 attempts with jittered exponential backoff under one wall-clock budget | **New** | `DatabaseExecUtils`, `DatabasesLandlord` |
| R7 | The script is told whether the deletion is hard or soft | **Missing** | `DatabasesLandlord`, `DatabaseExecUtils`, tests |
| R8 | Exhausted retries are visible to an operator without log spelunking | **Missing** | `DatabasesLandlord`, `AlertType` |
| R9 | Retries and the child process stop on server shutdown | **Missing** | `DatabasesLandlord` **and** `DatabaseExecUtils` |
| R10 | A timed-out or exited script cannot hang the hook task permanently | **Contradicts-code** — reachable on the default happy path | `DatabaseExecUtils` |
| R11 | Delivery semantics are documented truthfully in the config description | **Missing** | `DatabaseConfiguration` |
| R12 | Hook execution is concurrency-capped | **Missing** — promoted from an open question to a requirement under review | `DatabasesLandlord` |
| R13 | The user-supplied arguments value is never written to the log or into an exception message | **Contradicts-code** — it is logged today, by default | `DatabaseExecUtils` |
| R14 | The argument contract is unambiguous for database names beginning with `-` | **Missing** | `DatabaseExecUtils`, `DatabaseConfiguration` description |

### Hard invariants

**The hook must never be awaited by `HandleClusterDatabaseChanged`.** `ClusterStateMachine.ExecuteAsyncTask` calls `_rachisLogIndexNotifications.NotifyListenersAbout(index, error)` in a `finally` *after* awaiting the handler, and `DeleteDatabasesOperation` waits on exactly that notification with a 15s budget. Awaiting a hook that can now live for a multi-attempt retry budget would blow that window and turn a successful delete into an apparent failure. R6 ("wrap the execution in a retry") and R9 ("stop on shutdown") both create pressure toward awaiting; they must be satisfied without it.

**Nothing derived from the cluster read transaction may be touched inside the spawned task.** The hook site sits inside `using (context.OpenReadTransaction())` and `using (var rawRecord = ...)`, both of which close before the `Task.Run` body runs. `RawDatabaseRecord.Dispose()` nulls its backing blittable. Every value the hook needs must be extracted to plain managed locals at the call site. The codebase states this invariant explicitly at the notification boundary: `Debug.Assert(changeState.ContainsBlittableObject() == false, "You cannot use a blittable in the command state, since this is handled outside of the transaction")`.

## Behavior & approach

### What already works and should be kept

The delete hook is evaluated in `DatabasesLandlord.HandleClusterDatabaseChanged`, on the leader only, after the record-is-null early return and **above** the sharded/non-sharded dispatch to `HandleSpecificClusterDatabaseChanged`. That placement is what makes R3 true, but the reason is narrower than it looks: `DeleteDatabaseCommand`'s constructor normalizes `DatabaseName` to the base name, and `UpdateDatabase` notifies with the command's own name. The outer handler is *not* generally shard-agnostic — `ClusterStateMachine.ReadRawDatabaseRecord` deliberately accepts shard-suffixed names — so R3 rests on the command's normalization, not on the handler's contract. A sharded whole-database delete does fire exactly once with the base name, because every shard topology is blanked and the sharded branch of `EntireDatabasePendingDeletion()` then returns true.

Worth recording: nothing filters the cluster notification by node relevance, so the leader fires the hook even for a database it does not host. That is what makes the hook usable in a multi-tenant cloud cluster, where the script talks to an API rather than to local files.

The trigger is `type == nameof(DeleteDatabaseCommand)` combined with `RawDatabaseRecord.EntireDatabasePendingDeletion()`, evaluated synchronously at the call site. The second condition suppresses the hook for a replication-factor shrink — **but only while at least one node remains in the topology.** Removing the *last* node takes the topology to zero, and the non-sharded branch then short-circuits to true, so the hook fires. Any test asserting "shrink does not fire" must leave ≥2 nodes.

Execution goes through `DatabaseExecUtils.ExecuteOnDatabaseEvent`, following the house extension-point shape: `UseShellExecute = false`, both streams redirected and drained before waiting, `WaitForExit` with the configured timeout, `InvalidOperationException` on start failure / timeout / non-zero exit, `Process.Dispose()` in a `finally`. The argument contract is user arguments verbatim, then the escaped database name, then the base64 name. The escaped-plus-base64 pair answers the issue's own encoding concern and is strictly better than the sibling `DirectoryExecUtils`, which interpolates the database name unescaped.

### Change 1 — remove the create hook (R5)

Delete the three `Databases.OnDatabaseCreate.*` properties, the `AddDatabaseCommand` branch of the hook block, `RunDatabaseCreateExec`, and `OnDatabaseCreateExecTest`.

The fallout is **compile-only and fully enumerated**: a repo-wide search across all file types finds references in exactly those three files. No configuration-enumeration test breaks — the reflection-based config tests either assert naming conventions only (`RavenDB_20092`), filter to scopes these keys don't have (`RavenDB_16590`, `DatabaseSettings` filter `ServerWideOrPerDatabase`), or assert nothing about content (`RavenDB_5042`). None asserts a key list or a count. The Studio's `configuration.ts` is generated by `tools/TypingsGenerator` and gitignored. `RavenConfiguration` performs no unknown-key validation, so a stale `Databases.OnDatabaseCreate.*` left in an operator's `settings.json` after upgrade is silently ignored rather than fatal.

#### Why the create hook is dropped

`type == nameof(AddDatabaseCommand)` is not "a database was created":

- `AddDatabaseCommand` is also the command behind `UpdateDatabaseOperation` and `PUT /admin/databases` with an `ETag` header — any wholesale record update fires the "create" hook.
- `NotifyDatabaseAboutChanged` sits in the method's `finally`, so a create that throws (license limit, `RachisConcurrencyException`, validation) still notifies.
- Restore saves the record twice, so a normal restore fires it **twice** and a sharded restore **N+2** times.
- The branch's own comment claims topology updates don't count as creation. The code does not implement that claim.

A correct create hook is buildable — `AddDatabase` computes the `databaseExists` bit, `NotifyDatabaseAboutChanged` accepts a `changeState`, and `RecordRestored` is available and unconsulted — but it is out of scope.

### Change 2 — bounded, jittered retry (R6)

The first draft of this section was wrong in four ways; the corrected design follows.

**Fix R10 and R13 first — they are prerequisites, not hygiene.** With R10 unfixed, "3 attempts" silently degrades to "one attempt, then a permanently blocked thread": the stdout/stderr helpers block on `.Result` with no timeout, and that path is reached on the **default-configured normal-exit path** (the `Operations` log line), not just on timeout. `Process.WaitForExit(int)` does not drain redirected pipes, so a surviving grandchild holding the pipe open blocks forever and the retry loop never reaches attempt 2.

**One budget, not a multiplied timeout.** Today `TimeoutInSec` is applied *three times sequentially* per attempt — `WaitForExit(timeout)`, then `readStdOut.Wait(timeout)`, then `readErrors.Wait(timeout)` — so a single attempt is a 90s budget, and three attempts is ~275s, or unbounded in the pipe-held case. Collapse this to **one `CancellationTokenSource` per attempt** covering the process wait and both stream reads, and a **single wall-clock budget across all attempts**, linked to `ServerShutdown`. A new `Databases.OnDatabaseDelete.Exec.MaxRetryDurationInSec` (suggested default 90s) expresses the total honestly; deriving the bound by multiplication is what produced the first draft's false "inside a minute" claim.

**Make the attempt itself async, not just the backoff.** Requiring an async backoff while the attempt blocks a pool thread for the full timeout saves ~5s of thread occupancy out of ~95s, and it is worse than it looks: after `await Task.Delay(...)` the continuation must be re-scheduled onto a pool worker, and under the pool saturation this feature itself causes (R12), that latency is unbounded — so the nominal backoff is not the delay you get. `Raven.Server` targets `net8.0`, so `Process.WaitForExitAsync(token)` and `ReadToEndAsync(token)` are available and remove the blocking entirely, satisfying R6/R9/R10 together. Note these APIs appear nowhere in the repo today, so this is new ground rather than a thin change; the alternative, if that is unwelcome, is the house pattern of `PoolOfThreads.GlobalRavenThreadPool.LongRunning` plus the R12 cap.

**Jitter is required, not optional.** A bulk delete issues one `DeleteDatabaseCommand` per name in a tight loop, so N hooks fire at effectively the same instant. If the cloud API is the cause of failure, a fixed 1s/4s backoff makes all N retry in lockstep — a synchronized retry storm against the endpoint that was already struggling, in this feature's primary use case. Use decorrelated jitter: `delay = random(0, min(cap, base * 2^n))`, base 1s, cap 8s.

**Do not retry indiscriminately.** The first draft claimed the failure modes are "not distinguishable as permanent". That is false about the world and only true of the current code, which catches `Exception` and rewraps it as `InvalidOperationException`, destroying the `Win32Exception.NativeErrorCode` that identifies `ERROR_FILE_NOT_FOUND`, access-denied and bad-exe-format — all deterministically permanent. Preserve the inner exception and do not retry those; a typo in the exec path otherwise costs three process-start attempts and six log lines per deletion, forever.

**Retrying a timeout needs a gate.** `Kill()` is not a tree kill, so attempt 2 launches while attempt 1's grandchildren are still alive — up to three concurrent live process trees per database, all holding the inherited pipes that cause the R10 hang. Either exclude timeout from retry, or require `Kill(entireProcessTree: true)` plus confirmed exit before the next attempt, charge the elapsed timeout against the total budget, and never retry a timeout during shutdown.

**Consequence to document:** retries make delivery *at-least-once per event*, so operator scripts must be idempotent. That was already true (repeat delete requests, leader transitions); retries make it unavoidable.

**Honest limit:** retries cover *script* failure. They do not close the *event-loss* window — the leader gate is evaluated on a post-commit `Task.Run` continuation, and a leadership change or crash between apply and process start drops the event permanently with nothing persisted. Three attempts do not change that.

### Change 3 — communicate hard vs soft delete (R7)

`EntireDatabasePendingDeletion()` returns true for a whole-database **soft** delete as well as a hard one: the non-sharded branch returns as soon as `Topology.Count == 0` and never inspects the status. So today the script runs identically whether or not data was removed, and cannot tell.

**Source the kind from `changeState`, not from `DeletionInProgress`.** `HandleClusterDatabaseChanged` already receives the deserialized command as `object changeState` and ignores it on this path; `changeState as DeleteDatabaseCommand` yields `.HardDelete` as a plain `bool`, plus `.FromNodes` and `.ShardNumber`. It is guaranteed blittable-free by the assert quoted under Hard invariants, so it is safe to capture into the task. Specify a fallback for when the cast fails.

The first draft's suggestion — read the record's `DeletionInProgress` — is the worse option and would introduce a crash. It is a per-node dictionary that can hold **mixed** Hard and Soft values (a stuck earlier per-node delete alongside the whole-database one), can be **empty** while the hook still fires (a repeat delete adds no entries), and uses different key formats for sharded and non-sharded. Worse, it is lazily materialized off the record's blittable: in the common non-sharded whole-database delete, `EntireDatabasePendingDeletion()` short-circuits on `Topology.Count == 0` without ever populating that cache, so reading it inside the spawned task dereferences a nulled `_record` — a `NullReferenceException` in a fire-and-forget task. The sharded branch reads the dictionary first and would cache it, so the bug would be **non-sharded-only and would survive a sharded test**.

Both kinds should still fire — from a cloud-account perspective the database is gone either way — with the kind passed so the consumer can decide.

### Change 4 — argument contract (R7, R14)

The contract becomes: user arguments, then `--`, then escaped database name, then base64 name, then deletion kind.

The `--` end-of-options sentinel is not decoration. `-` is a legal database-name character with no leading-position restriction, so `--force` and `-rf` are valid database names; they pass through `EscapeSingleArg` unquoted (it only quotes on whitespace) and arrive as bare positionals that `getopt`, `argparse` or a PowerShell `param()` block will parse as flags. Metacharacter *injection* is unreachable — there is no shell, and the validator rejects whitespace, quotes and semicolons — but *argument confusion* is reachable, and `--` closes it.

**This is a breaking change to the two test scripts, in the same commit.** `OnDatabaseDeleteExecTest`'s PowerShell body declares exactly three parameters; a fourth positional is unbindable and fails the script. The bash variant would silently ignore it. "Nothing has shipped so the contract is free to change" is true of consumers, not of the tests.

### Change 5 — operator visibility (R8, R11, R13)

**R13 — stop logging the secret.** `isSecured: true` is not a safeguard against this: it has exactly two consumers, both in the settings-API response, and there is no log-redaction mechanism anywhere in the server. `DatabaseExecUtils` logs the full command line — including the verbatim `.Arguments` value — at `Operations`, which is the **default** log mode, and every exception message repeats it. Because `/admin/logs/watch` and `/admin/logs/download` are `Operator`-level while the settings API withholds a secured value even from a ClusterAdmin, an Operator can read from the logs a credential the API refuses to show anyone. Log the executable, the database name, the base64 name and an argument *count*; never the arguments themselves. Script stdout/stderr is logged on the same line, so the description should warn that a script echoing tokens leaks them too. (The same defect exists in `SecretProtection` and `DirectoryExecUtils`; that is precedent, not justification. The debug package is clean — that path redacts and skips server-wide-only entries.)

**R8 — alert on exhaustion, server-scoped.** Add a NotificationCenter alert when retries are exhausted, and raise it with **`database: null`**. The two precedents in the same method are database-scoped, which would be self-defeating here: the deletion path calls `DeleteDatabaseNotifications`, which drops the entire per-database notifications table, and with a retry budget the exhaustion alert lands *after* that on the node hosting the database. A new member in `AlertType` is required; there is no existing generic external-script alert to reuse.

Note the justification, corrected: the alert is warranted for actionability and aggregation, **not** because the log line can be suppressed. There is no log level below `Operations` other than `None`, so the first draft's "invisible in any deployment that lowers its log level" was wrong.

**Logging detail.** A failing attempt emits *two* `Operations` lines — one from the exec helper, one from the landlord's catch, whose exception message repeats the whole command line — so three attempts yield up to six. Log per-attempt failures at `Info` and only final exhaustion at `Operations`. Nothing currently logs *why* the hook fired; `index`, `type` and `changeType` are all in scope at the call site and reach neither `Run*Exec` method. (Also: the success log line is only reached when the process exited in time *and* both stream reads completed — start-failure and timeout throw before it.)

**R11.** Extend the `.Exec` `[Description]` to state: leader-only; fires when deletion *starts*, not when it completes; best-effort, may be delivered more than once and may be lost across a leadership change; the script must be idempotent; the child inherits the server's environment.

### Change 6 — concurrency cap (R12)

Promoted from an open question to a requirement. There is no cap today: N deletions produce N `Task.Run`s, each blocking a pool worker. `Program.cs` sets min threads to 2 × `ProcessorCount`, so on an 8-core instance deleting 100 databases against a hung script demands 100 workers against 16, with the rest injected at roughly 2/s — about 40s of pool saturation before the last hook starts, plus 100 concurrent interpreter processes.

The consequence that makes this a requirement rather than a tuning note: pool saturation delays `ClusterStateMachine.ExecuteAsyncTask`, whose `finally` is what calls `NotifyListenersAbout` — the notification every cluster write awaits, including the delete path's own 15s wait. **A hung hook can starve the pool badly enough to time out unrelated cluster operations.** Use a `SemaphoreSlim` (suggested default 8, mirroring `Databases.MaxConcurrentLoads`) and drop rather than queue when saturated, since delivery is already best-effort.

Correction to the first draft: peak concurrent blocked threads is N, not 3N — only one attempt per database is ever in flight. What retries triple is blocked-thread-seconds and process launches.

### Change 7 — shutdown coordination (R9)

The first draft's requirement was actively harmful. Corrected:

- **Do not register the hook with `_disposing`.** `AsyncGuard.CloseAndLock()` blocks with no timeout until every visitor exits, and `DatabasesLandlord.Dispose()` calls it as its first statement — so a hook holding the guard across a retry budget stalls landlord disposal and everything ordered after it (`_env`, the leader request executor, the context pool, the file locker). With R10 unfixed that stall is unbounded. The existing `Task.Run` deliberately escapes the guard; all eight other `TryEnter` sites are strictly synchronous scopes.
- **Capture `ServerShutdown` once at hook entry.** Ordering is fine — the shutdown token is cancelled before the landlord is disposed — but the CTS itself is *disposed after* the landlord, and reading `.Token` on a disposed source throws `ObjectDisposedException`. So capture once and pass it down; never re-read the property inside the retry loop.
- **R9 lives in `DatabaseExecUtils` too.** `Process.WaitForExit(int)` is not cancelable, so a token can only act between attempts unless something kills the process. `ExecuteOnDatabaseEvent` needs a `CancellationToken` parameter plus either `WaitForExitAsync(token)` or `token.Register(() => ProcessExtensions.TryKill(process))`.
- **"Awaited in `Dispose()`" needs new state.** There is no registry of in-flight hook tasks. If a drain is wanted, track live tasks and do a *bounded* wait (≈5s) **after** `CloseAndLock()`, abandoning on timeout.
- **Swallow cancellation.** Once the loop observes shutdown, `Task.Delay` throws `OperationCanceledException` into the landlord's `catch (Exception)`, producing an `Operations`-level "failed to execute" line per pending delete on every clean restart. Add an explicit `catch (OperationCanceledException) when (token.IsCancellationRequested) { }`.

### Change 8 — remaining hygiene

- Extract everything the hook needs into plain locals at the call site, inside the `using` scope — the config-emptiness check being the cheapest of them. The first draft framed this as avoiding a `Task.Run` allocation; that framing was both wrong and beside the point. The allocation is already gated by the leader check, the command type and `EntireDatabasePendingDeletion()`, so it is one `Task` per whole-database deletion on the leader — hygiene, not a performance edge. The *real* reason to hoist is the use-after-dispose hazard under Hard invariants.
- Guard the `Kill()` call; `ProcessExtensions.TryKill` already exists for exactly this and is unused here. Today an exception from `Kill()` means the diagnostic exception is never constructed and stdout/stderr are lost.
- Add the missing `/// <summary>` comments; all 13 pre-existing properties in `DatabaseConfiguration` have them and the 6 new ones have none. This is convention only — `CS1591` is in `NoWarn`, so citing warnings-as-errors here would be wrong.
- Optionally extract the body shared with `DirectoryExecUtils.OnDirectoryInitialize` (~70 lines, not the ~90 first claimed). The duplication has already drifted in both directions: the empty-arguments leading-space defect was fixed only in the copy, and the unescaped-database-name defect exists only in the original. Scope-widening; call it separately.

## Dependencies & integration points

- **Consumes:** the cluster database-changed notification path (`NotifyDatabaseAboutChanged` → `ClusterChanges.OnDatabaseChanges` → `ServerStore.TriggerDatabases` → `DatabasesLandlord.ClusterOnDatabaseChanged`); `RawDatabaseRecord.EntireDatabasePendingDeletion`; `RachisState` via `ServerStore.IsLeader`; `CommandLineArgumentEscaper`; `ResourceNameValidator`.
- **Affects:** nothing else in the server. Additive and inert unless configured.
- **Cloud side — the missing half.** No RDBCL ticket exists. The cloud needs a script on instance hosts, wired to `Databases.OnDatabaseDelete.Exec`, calling a cloud API endpoint that registers the deleted database for the 2-week retention sweep. Neither exists or is specified; the existing retention machinery traces to RDBCL-873. **The RavenDB-side change delivers no customer value until this exists.**
- **Alternative considered, not taken:** a `DatabaseChanged` server notification already exists, so the cloud could subscribe rather than have the server exec a script. Rejected upstream by ppekrol's RavenDB-22546 comment; recorded for traceability.

## Data model / config changes

No schema, persisted-data or wire-format change. Configuration only, all `ConfigurationEntryScope.ServerWideOnly`:

| Key | Type | Default | Notes |
|---|---|---|---|
| `Databases.OnDatabaseDelete.Exec` | string | null | `EXPERT:` description carrying the argument contract and the delivery guarantee (R11) |
| `Databases.OnDatabaseDelete.Exec.Arguments` | string | null | `isSecured: true` — redacted in the **settings API only**; *not* redacted in logs (see R13) |
| `Databases.OnDatabaseDelete.Exec.TimeoutInSec` | `TimeSetting` | 30 | `[TimeUnit(TimeUnit.Seconds)]`; per-attempt cap |
| `Databases.OnDatabaseDelete.Exec.MaxRetryDurationInSec` | `TimeSetting` | 90 | **new** — total wall-clock budget across all attempts (R6) |

Removed: the three `Databases.OnDatabaseCreate.*` counterparts.

`TimeSetting` property names must not carry a unit suffix while their keys must end in `InSec` — enforced by reflection over every `ConfigurationCategory` subclass in `test/FastTests/Issues/RavenDB_20092.cs`. Existing naming satisfies it; the new key must too.

## Cross-cutting concerns / angles

- **Backward compatibility:** no consumer impact — nothing has shipped. One first-draft claim was wrong and is withdrawn: server-wide-only keys are **not** unreachable from the database-settings API, so a database record *can* carry one (see Security). Stale `Databases.OnDatabaseCreate.*` entries in an operator's `settings.json` are silently ignored, since `RavenConfiguration` does no unknown-key validation.
- **Performance:** not negligible under bulk teardown — see Change 6. In steady state the hook is off unless configured and deletion is rare.
- **Security & permissions:**
  - **Correction to the first draft.** `ServerWideOnly` is *not* enforced at `PUT /databases/*/admin/configuration/settings`. That processor validates only three storage keys, audits key names, and persists whatever it is given; enforcement happens later, at per-database `RavenConfiguration.Initialize()`, which throws "can only be set at server level". The reflection-based rejection in `DatabaseHelper` exists but is wired only into the restore paths. So a DatabaseAdmin can persist `Databases.OnDatabaseDelete.Exec` into a DatabaseRecord and thereby make that database **fail to load** — a footgun recoverable only by another settings PUT. Adding a scope check to the put-settings processor (reusing `DatabaseHelper.GetServerWideOnlyConfigurationKeys()`) would be a genuine improvement worth doing.
  - **Why there is nonetheless no code-execution path** — the load-bearing fact, which the first draft missed: `RunDatabaseDeleteExec` reads `_serverStore.Configuration.Databases`, the *server* configuration object, never the per-database one. A value smuggled into a DatabaseRecord is never executed.
  - **Keep `ServerWideOnly` permanently.** The first draft framed widening as a checklist item ("add the keys to the non-cluster-admin blocklist"). That mitigation is weaker than it looks: the blocklist is gated on `Security.RestrictExternalScriptUsageForNonClusterAdmin`, which **defaults to false**, and `PUT /admin/databases` (Operator-level) never inspects `Settings` for blocklisted or server-wide keys at all. So on a default server a DatabaseAdmin already has arbitrary code execution via the per-database `Storage.OnDirectoryInitialize.Exec`. Widening these keys would require guarding both paths and would still be off by default.
  - **Secret exposure in logs (R13)** — the highest-severity finding in this review. Detail under Change 5.
  - **Argument confusion via leading `-` (R14)** — detail under Change 4. Note also that `ResourceNameValidator` is fragile in a way worth knowing: it validates with the *index*-name regex (which additionally permits `/`, excluded only downstream by an invalid-filename-char check) and uses Unicode-wide `char.IsLetterOrDigit`.
  - **Environment inheritance — confirmed.** `ProcessStartInfo.Environment` is never touched, and with `UseShellExecute = false` .NET seeds the child from the current process. The server's environment is exactly where `RAVEN_`-prefixed secrets live, including the license and certificate/master-key settings. State this in the description; the operator's script is the trust boundary.
- **Observability:** see Change 5.
- **Failure & rollback:** the script cannot affect the deletion (R4). Residual accepted risks:
  - **Fires at deletion start.** A stalled or failed delete leaves the cloud having scheduled retention for a live database. Mitigated by the 2-week delay, not by design.
  - **Event loss.** Leadership change or crash between raft apply and process start drops the event silently; `RachisState.LeaderElect` means an entry applied across a term change can find no node reporting leadership. This is the one failure mode that reproduces RavenDB-22546 itself.
  - **Duplicates.** A repeated delete request re-fires the hook (no new `DeletionInProgress` entries, but `Topology.Count == 0` short-circuits true again) — realistic when a hard delete is stuck on an offline node and the user retries. Plus retry duplicates. Scripts must be idempotent.
  - **Unobserved removal paths.** Records removed via the cluster-shrink path, or via `UpdateDatabase`'s inline `DeleteDatabaseRecord` (which notifies, but the landlord returns early on the null record), fire nothing. In `RemoveNodeFromCluster`'s last-node branch **no notification is raised at all**. Retries cannot help where nothing fires.
  - **Pre-existing orphans.** Backups of databases deleted before this ships are unaffected. A cloud-side reconciliation sweep is the only recovery, and would backstop every bullet above.
- **Project-specific:** commits `RavenDB-#### Description`; no tabs in `.cs`; `[RavenFact]`/`[RavenTheory]` over plain xUnit; explicit types over `var`. Config reference docs are a separate RDoc deliverable.

## Testing strategy

Existing coverage is one test (`OnDatabaseDeleteExecTest`, `RavenTestCategory.Configuration`) asserting the output file exists and contains both name forms. Single-node, hard-delete-only, `Contains`-based, so blind to duplicates.

**Prerequisite:** deterministic count assertions need a test hook. `DatabasesLandlord` already has the house `ForTestingPurposesHolder` pattern; add an invocation counter / completion signal there. Without it, proving "no second invocation" against a fire-and-forget task means a fixed sleep, which is inherently flaky.

- **Error semantics** — missing/unstartable executable, non-zero exit, script outrunning the timeout. Each must assert the deletion still completes; that guarantee is currently untested and is the feature's most important property.
- **Retry** — fails-twice-then-succeeds runs exactly 3 times; always-fails is attempted exactly 3 times; a permanent start failure is attempted **once**; the total budget is respected.
- **Timeout key** — never set by any test today. Exercise a short timeout and assert the process is killed (tree death is not assertable while `Kill()` is not a tree kill).
- **Hard vs soft** — both fire; the kind argument reflects which.
- **Argument contract** — a name beginning with `--` round-trips intact through the `--` sentinel, in both the plain and base64 positions. This is the real edge case, not merely "a name at the edge of the allowed set". Both test script bodies must accept the new arity.
- **Exactly-once shape** — counts, not containment. Sharded database fires once with the base name; a replication-factor shrink **leaving ≥2 nodes** does not fire.
- **Leader gate** — feasible and worth doing: `ClusterTestBase.CreateRaftCluster` accepts a per-node `customSettingsList` plus an explicit `leaderIndex`, so each node gets a distinct output path and only the leader's file should exist. Two caveats: when `customSettingsList` is supplied the default cluster settings are **not** merged in, so the test must add `Cluster.ElectionTimeout` itself; and `ExtensionPointsTests` derives from `RavenTestBase`, not `ClusterTestBase`, so the cluster test needs a **new class** — "extend the existing file" is not achievable for this case.
- **Negative** — unconfigured, empty and whitespace-only exec produce no process and no error (values are trimmed, so whitespace short-circuits).
- **Categories** — single-node stays `Configuration`; the leader-gate test is `Cluster | Configuration`; the sharded test is `Sharding | Configuration`. Note `Sharding` auto-skips on 32-bit platforms, so the sharded exactly-once assertion silently vanishes there.
- **Config removal** — nothing references the removed create keys.

Platform note: the current tests carry no platform skip and depend on `bash` on PATH or Windows PowerShell 5.1 with a permissive execution policy. The copied `chmod 700` call is dead weight and un-awaited — the interpreter receives the script as an argument. Don't propagate it.

## Risks & review findings

Confirmed by adversarial review and folded in. Findings that **corrected the first draft** are marked ✗.

| Finding | Where it landed |
|---|---|
| ✗ `isSecured` does not redact logs; Operator can read via `/admin/logs/*` a secret the settings API withholds from ClusterAdmin | R13, Change 5 |
| ✗ `ServerWideOnly` is not enforced at the settings PUT; the key persists and the database then fails to load | Security; the no-execution guarantee restated on the correct basis |
| ✗ `DeletionInProgress` is the wrong source for the delete kind and would NRE in the non-sharded path only | Change 3 — switched to `changeState` |
| ✗ Registering the hook with `_disposing` would hang shutdown unboundedly | Change 7 |
| ✗ Per-attempt bound is 3 × `TimeoutInSec`, so 3 attempts is ~275s, not 95s | Change 2, new budget key |
| ✗ The `.Result` hang is reachable on the default happy path, not only on timeout | R10 promoted to prerequisite |
| ✗ Permanent start failures *are* distinguishable; the code destroys the evidence | Change 2 |
| ✗ Concurrency cap is a requirement — pool starvation can time out unrelated raft notifications | R12, Change 6 |
| ✗ No log level below `Operations`, so the alert must be argued on actionability | Change 5 |
| ✗ Database names may begin with `-`; argument confusion is reachable | R14, Change 4 |
| ✗ The hook must never be awaited, or the delete's 15s confirmation window breaks | Hard invariants |
| ✗ `rawRecord`/context are disposed before the task body runs | Hard invariants, Change 8 |
| Retrying timeouts accumulates live process trees under a non-tree `Kill()` | Change 2 |
| No jitter ⇒ synchronized retry storm against the cloud API | Change 2 |
| Database-scoped alert would be destroyed by the deletion that triggers it | Change 5 |
| Argument-contract change breaks both existing test scripts | Change 4 |
| RF-shrink suppression only holds while ≥1 node remains | Behavior; testing item corrected |
| R3 rests on `DeleteDatabaseCommand`'s normalization, not on handler shard-agnosticism | Behavior |
| `RemoveNodeFromCluster` last-node branch raises no notification at all | Failure & rollback |
| Create-hook removal fallout is complete and compile-only; no config-enumeration test breaks | Change 1 |

Residual risks: the event-loss window and the fire-at-initiation race remain open **by decision**; pre-existing orphaned backups need cloud-side reconciliation; and the feature delivers zero customer value until the cloud consumer is built.

## Open Questions / Assumptions

1. **Blocking for customer value, not for this code:** who owns the cloud-side consumer, and does an endpoint for "register this deleted database for backup retention" exist or need building? No RDBCL ticket exists.
2. **Assumption:** total retry budget 90s, per-attempt cap 30s, decorrelated jitter with 1s base and 8s cap. Adjust to the cloud script's real failure profile.
3. **Assumption:** both hard and soft deletes fire, with the kind passed. If the cloud decides soft deletes must not trigger retention, that filter belongs in the script.
4. **Open, non-blocking:** whether to add the scope check to the put-settings processor to turn the fails-to-load footgun into a clean rejection.
5. **Open, non-blocking:** whether to adopt `WaitForExitAsync` (new to this repo) or the existing `PoolOfThreads` long-running pattern.
6. **Open, non-blocking:** whether to extract the shared exec helper now, fixing two divergent defects in one place.
7. **Assumption:** target `v6.2`, merged forward — consistent with the branch base and with house practice.

## Out of scope

- The create/`AddDatabaseCommand` hook and the `ClusterStateMachine` plumbing that would make it correct.
- A post-deletion-completed hook; closing the event-loss window with persisted pending events.
- Cloud-side work: consuming script, API endpoint, reconciliation sweep.
- Retrofitting `Storage.OnDirectoryInitialize.*`, and the identical log-leak in `SecretProtection`, beyond the optional shared-helper extraction.
- Studio UI (server-wide keys only).
- docs.ravendb.net configuration reference (separate RDoc deliverable).

## References

- Affected areas: `DatabaseConfiguration`, `DatabasesLandlord.HandleClusterDatabaseChanged` / `RunDatabaseDeleteExec`, `DatabaseExecUtils.ExecuteOnDatabaseEvent`, `AlertType`, `test/SlowTests/ExtensionPoints/ExtensionPointsTests.cs`, plus a new cluster test class
- Read for grounding, not modified: `ClusterStateMachine` (`AddDatabase`, `UpdateDatabase`, `RemoveNodeFromDatabase`, `RemoveNodeFromCluster`, `NotifyDatabaseAboutChanged`, `ExecuteAsyncTask`), `RawDatabaseRecord`, `DeleteDatabaseCommand`, `PutDatabaseSettingsCommand`, `ConfigurationCategory`, `ConfigurationEntryValue`, `DatabaseHelper`, `AbstractAdminConfigurationHandlerProcessorForPutSettings`, `SecurityConfiguration`, `StorageConfiguration`, `DirectoryExecUtils`, `ProcessExtensions`, `CommandLineArgumentEscaper`, `ResourceNameValidator`, `AsyncGuard`, `LoggingSource`, `AdminLogsHandler`, `AdminDatabasesHandler`, `ClusterTestBase`
- Commits: `603d71485b1`, `f157862294b` (branch `copilot/ravendb-25293-add-database-events-hooks`, rebased onto `03e4c3b2ff1`)
- Related issues: RavenDB-22546 (originating cost leak), RavenDB-16258 (external-script security precedent), RavenDB-12161 (first exec extension point), RDBCL-873 (existing cloud backup retention)
