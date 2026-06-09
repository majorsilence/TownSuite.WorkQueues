# Changelog

All notable changes to this project will be documented in this file.

---

## [2.1.0] — 2026-06-09

### New features

- **Redis backend** (`TownSuite.WorkQueues.Redis`) — `RedisWorkQueue` (Redis Lists, FIFO) and `RedisMessageBus` (Redis Streams with consumer groups, automatic retry, dead-lettering, and `XAUTOCLAIM`-based reclaim). No database required.

---

## [2.0.0] — 2026-06-09

### Breaking changes

- **Serialisation format changed.** `DbBackedWorkQueue` now uses `System.Text.Json` instead of Newtonsoft.Json. Payloads are stored as compact JSON without `$type` metadata. Messages written by v1.x **cannot be deserialised** by v2.x. Drain the queue before upgrading.

- **`Newtonsoft.Json` removed** from `TownSuite.WorkQueues`. If your application depended on the transitive reference, add an explicit package reference.

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
