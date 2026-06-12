# Changelog

All notable changes to this project will be documented in this file.

---

## [2.3.0] — 2026-06-12

### Bug fixes

- **Duplicate `Subscribe` calls no longer cause double-dispatch** — Handler storage changed from `ConcurrentBag<Func>` to `ConcurrentDictionary<object, Func>` keyed on the consumer instance. Calling `bus.Subscribe(consumer)` twice silently no-ops instead of registering the same handler twice and delivering every message twice.

- **`AllowEmptyBatches` was not honoured** — The between-poll `Task.Delay` was always executed regardless of the option value. Fixed by extracting a `WaitAsync()` helper in each bus that checks the flag before delaying.

- **`LegacyJsonDeserializer` `$values` false positives** — The fallback that unwrapped `$values` arrays could trigger on malformed payloads that contained `$values` without `$type`. The guard now requires both fields to be present before attempting the fallback.

### New features

- **`IMessageBus.IsPolling`** — New read-only property on the interface and all three bus implementations. Returns `true` while the background polling loop is alive and `false` if it has stopped unexpectedly. Use this to implement health checks:
  ```csharp
  builder.Services.AddHealthChecks()
      .AddCheck("message-bus", () =>
          sp.GetRequiredService<IMessageBus>().IsPolling
              ? HealthCheckResult.Healthy()
              : HealthCheckResult.Unhealthy("Bus polling loop has stopped"));
  ```

- **`IMessageBus.ReplayDeadLettered<T>()`** — Resets `failedat` and `retrycount` for all dead-lettered messages of a given type so they are redelivered on the next polling cycle. Returns the count of rows (or stream entries for Redis) that were replayed:
  ```csharp
  int replayed = await bus.ReplayDeadLettered<OrderSubmitted>();
  ```
  The Redis implementation reads the `{stream}:dead` key and re-enqueues each entry to the main stream.

- **`ConsumeContext<T>.CancellationToken`** — New default interface property (returns `CancellationToken.None` for custom implementations). All three bus backends now pass their shutdown `CancellationToken` to consumers via `SimpleConsumeContext<T>`, so consumers doing async I/O can observe graceful shutdown:
  ```csharp
  public async Task Consume(ConsumeContext<OrderSubmitted> context)
  {
      await _db.SaveAsync(context.Message, context.CancellationToken);
  }
  ```

- **`IRedisWorkQueue` gains `CancellationToken` parameters** — `EnqueueAsync` and `DequeueAsync` now accept an optional `CancellationToken cancellationToken = default`. No breaking change — existing callers compile unchanged.

- **`BatchOptions.ContinuousPolling`** renames `AllowEmptyBatches`. The old name is kept as an `[Obsolete]` computed alias that redirects to `ContinuousPolling`. The new name accurately describes the behaviour: "poll again immediately without waiting `MaxWaitTime` when the last batch was empty."

### Production readiness

- **`Task.Yield()` in bus constructors** — Ensures `Subscribe()` calls made immediately after `new PostgresMessageBus(...)` are registered before the first polling cycle executes, eliminating a rare race where the loop could fire before any handlers were registered.

- **`RedisOptions.ConsumerName` default includes `ProcessId`** — Default changed from `Environment.MachineName` to `$"{MachineName}-{ProcessId}"`. Multiple processes on the same host no longer share a pending-entry list, which previously caused incorrect retry counts and missed reclaims.

- **`AdminConnectionString` fallback** — `SqlTransportOptions` (Postgres) and `SqlServerTransportOptions` now fall back to `ConnectionString` when `AdminConnectionString` is not set. Migration services no longer require the admin string to be specified separately.

- **`AddPostgresMessageBus` DI extension added** — `PostgresMigrationHostedServiceExtensions` now exposes `AddPostgresMessageBus(Action<IServiceProvider, PostgresMessageBus>)`, matching the pattern already present in the SQL Server package.

- **`AddRedisMessageBus` optional `subscribe` callback** — The DI helper now accepts an optional `Action<IServiceProvider, RedisMessageBus>? subscribe` parameter for registering consumers in the factory, matching the Postgres/SqlServer pattern.

### Documentation

- XML documentation added to `MessageDto`, `DbBackedWorkQueue`, `DbBackedWorkQueue_NonDestructive`, and `BatchOptions` classes (previously undocumented).
- README updated: `ContinuousPolling` replaces `AllowEmptyBatches` in config table; `ReplayDeadLettered<T>` and health check patterns added to Dead-Letter section; `ConsumerName` default corrected; bus instantiation examples updated to `await using`.

---

## [2.2.0] — 2026-06-10

### New features

- **SQL Server message bus** (`TownSuite.WorkQueues.SqlServer`) — `SqlServerMessageBus` using `UPDLOCK + ROWLOCK + READPAST` for concurrent-safe polling; `SqlServerMigrationHostedService` for idempotent startup DDL; `SqlServerServiceExtensions` DI helpers. Requires SQL Server 2016+.

---

## [2.1.0] — 2026-06-09

### New features

- **Redis backend** (`TownSuite.WorkQueues.Redis`) — `RedisWorkQueue` (Redis Lists, FIFO) and `RedisMessageBus` (Redis Streams with consumer groups, automatic retry, dead-lettering, and `XAUTOCLAIM`-based reclaim). No database required.

---

## [2.0.0] — 2026-06-09

### Breaking changes

- **`Newtonsoft.Json` removed** from `TownSuite.WorkQueues`. If your application depended on the transitive reference, add an explicit package reference.

### Serialisation change (not breaking for most users)

`DbBackedWorkQueue` now writes payloads with `System.Text.Json` (compact JSON, no `$type`
metadata). **Reading** old Newtonsoft.Json payloads from the queue is handled automatically:

| Legacy payload type | Handled? |
|---|---|
| Simple POCO with `$type` annotation | ✓ — `$type` is silently ignored |
| Nested POCO, each level with `$type` | ✓ — ignored at every level |
| Collection root wrapped in `$values` (`List<T>`, `T[]`) | ✓ — `$values` array is extracted before deserialising |
| Polymorphic base-class usage (derived type written, base type read) | ✗ — derived-only properties are dropped; drain those channels before upgrading |

Draining the queue before upgrading is no longer required for the common case. The only
scenario that still requires a drain (or a manual replay) is polymorphic payloads where
the stored `$type` pointed to a concrete derived class and the call site deserialises to an
abstract base type — an unusual pattern.

- **Schema changes** (see migration notes below).

### Bug fixes

- **Crash in `PostgresMessageBus` polling loop** — `reader.GetDateTime(4)` was called on a column that is always `NULL` for unprocessed rows, throwing `InvalidCastException` on every poll cycle.

- **SQL parameter type mismatch in `PostgresMessageBus`** — The `channels` filter was passed as a comma-joined string but cast to `text[]`, causing a PostgreSQL type error. Now passed as a proper `string[]` via Npgsql.

- **Handler exceptions silently crashed the polling loop** — An unhandled exception thrown by any consumer would propagate through `Task.WhenAll`, roll back the entire batch, and restart the poll. All batched messages would retry indefinitely. Each message is now dispatched in its own `try/catch`; a failing message increments its retry count while successful messages in the same batch are still committed.

- **`PostgresMigrationHostedService` was entirely broken** — Four embedded-resource names contained the typo `WorkeQueue` instead of `WorkQueue`, causing every startup migration to throw `InvalidOperationException: Resource not found`. Fixed.

- **`DbBackedWorkQueue_NonDestructive.Dequeue` returned wrong result on SQL Server** — Reader was opened but never read; relied on output parameter value that isn't populated until after the reader closes. The method now reads from the result set first and falls back to the output parameter after reader disposal, covering both PostgreSQL (result-set OUT params) and SQL Server (post-close OUTPUT params).

### Security fixes

- **`TypeNameHandling.All` removed** — Newtonsoft.Json `TypeNameHandling.All` allows arbitrary type instantiation from attacker-controlled JSON. `DbBackedWorkQueue` now uses `System.Text.Json` with no type metadata.

### New features

- **Dead-letter queue** — Failed messages are retried up to `BatchOptions.MaxRetries` times (default 3). Once the limit is reached, `failedat` is set and the message is permanently excluded from polling. Dead-lettered rows remain in the table for inspection and can be replayed by clearing `failedat` and `retrycount`.

- **`BatchOptions.MaxRetries`** — new property (default `3`) that controls how many delivery attempts are made before dead-lettering.

- **`DbBackedWorkQueue` now implements `IWorkQueue`** — the base class carries the interface directly; `DbBackedWorkQueue_NonDestructive` retains it for documentation clarity.

- **Channel name validation** — `Enqueue` and `Publish` now throw if the channel name exceeds 500 characters.

### Performance

- **Partial index on `workqueue`** — Added `ix_workqueue_channel_unprocessed` on `(channel, timecreatedutc) WHERE timeprocessedutc IS NULL AND failedat IS NULL`, covering the hot polling query for both PostgreSQL and SQL Server.

### Schema migration notes

The `PostgresMigrationHostedService` applies all changes automatically on startup. For SQL Server, run the updated scripts in `scripts/sql-server/` manually.

| Change | SQL |
|---|---|
| Widen `channel` column | `ALTER TABLE workqueue ALTER COLUMN channel TYPE VARCHAR(500)` |
| Add `failedat` column | `ALTER TABLE workqueue ADD COLUMN IF NOT EXISTS failedat TIMESTAMP NULL` |
| Add `retrycount` column | `ALTER TABLE workqueue ADD COLUMN IF NOT EXISTS retrycount INT NOT NULL DEFAULT 0` |
| Add partial index | `CREATE INDEX IF NOT EXISTS ix_workqueue_channel_unprocessed ...` |

All PostgreSQL migration statements are idempotent (`IF NOT EXISTS`, `ADD COLUMN IF NOT EXISTS`, `DO` block guards).

---

## [1.0.3] and earlier

Initial releases. Work queue and message bus backed by PostgreSQL and SQL Server.
Message bus and consumer lifecycle were marked as work in progress.
