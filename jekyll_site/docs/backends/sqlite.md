---
layout: docs
title: SQLite Backend — TownSuite.WorkQueues
description: Use the SQLite backend for local development with multiple processes sharing a single database file.
permalink: /docs/backends/sqlite/
---

# SQLite Backend

<div class="callout">
<strong>Local development only.</strong> The SQLite backend is designed for scenarios where a frontend process enqueues work and a separate worker process consumes it — all on the same machine, sharing a single <code>.db</code> file. It is not intended for production deployments.
</div>

## When to use it

- You want to run a local dev environment without Docker or a database server.
- Your solution has multiple processes (e.g. a web frontend + a background worker) that need to share queue state.
- You want a drop-in substitute for the Postgres or SQL Server bus that requires zero infrastructure.

## Install

```sh
dotnet add package TownSuite.WorkQueues.Sqlite
```

## Register the bus

The migration hosted service creates the database file, sets WAL mode, and applies the schema on startup:

```csharp
builder.Services.AddSingleton(new SqliteTransportOptions
{
    ConnectionString = "Data Source=./workqueue.db",
    LockTimeout = TimeSpan.FromSeconds(60)  // default
});
builder.Services.AddSqliteMigrationHostedService();
builder.Services.AddSqliteMessageBus((sp, bus) =>
{
    bus.Subscribe(new OrderConsumer());
    // or DI-scoped:
    bus.Subscribe<OrderSubmitted, OrderConsumer>();
});
```

Both the frontend (publisher) and the worker (consumer) point at the same connection string. The database file is created the first time the migration service runs.

## Options reference

See [`SqliteTransportOptions`]({{ '/docs/configuration/' | relative_url }}#sqlite--sqlitetransportoptions) for the full property table.

The key SQLite-specific option is `LockTimeout`:

```csharp
new SqliteTransportOptions
{
    ConnectionString = "Data Source=./workqueue.db",
    LockTimeout = TimeSpan.FromSeconds(120),  // set to exceed longest consumer processing time
    MaxBatchSize = 50,
    MaxWaitTime = TimeSpan.FromSeconds(2),
    MaxRetries = 3
}
```

## How claiming works (visibility timeout)

SQLite does not support `FOR UPDATE SKIP LOCKED`. Instead, claiming is emulated with `lockeduntil` and `locktoken` columns. A single atomic `UPDATE` claims a batch of rows:

```sql
UPDATE workqueue
SET lockeduntil = @lockeduntil, locktoken = @locktoken
WHERE id IN (
    SELECT id FROM workqueue
    WHERE timeprocessedutc IS NULL
      AND failedat IS NULL
      AND (lockeduntil IS NULL OR lockeduntil < @now)
      AND (scheduledfor IS NULL OR scheduledfor <= @now)
      AND channel IN (...)
    ORDER BY timecreatedutc
    LIMIT @maxMessages
)
```

SQLite serializes all write operations, so only one process can execute this UPDATE at a time. Other pollers see `lockeduntil` already in the future and skip those rows.

## WAL mode

The migration service enables WAL (Write-Ahead Logging) on the database file:

```sql
PRAGMA journal_mode=WAL
```

WAL allows one writer and multiple concurrent readers to operate simultaneously — essential when a frontend process is publishing while a worker is consuming. Without it, concurrent access would serialize on file-level locks and produce `SQLITE_BUSY` errors. This setting is stored in the database file and only needs to be applied once.

## Crash recovery vs transactional backends

| | PostgreSQL / SQL Server | SQLite |
|---|---|---|
| Crash recovery | Transaction rollback — rows immediately available | `lockeduntil` expires — rows available after `LockTimeout` |
| Recovery speed | Instant | Up to `LockTimeout` (default 60 s) |

Set `LockTimeout` to comfortably exceed the longest expected consumer processing time. If a message takes 45 seconds to process and `LockTimeout` is 30 seconds, another process may claim it before the first delivery finishes.

## Inspecting the queue

```sql
-- All pending messages
SELECT id, channel, substr(payload, 1, 60) AS payload, timecreatedutc
FROM workqueue
WHERE timeprocessedutc IS NULL AND failedat IS NULL
ORDER BY timecreatedutc;

-- Currently claimed (in-flight)
SELECT id, channel, lockeduntil, locktoken
FROM workqueue
WHERE lockeduntil > datetime('now')
ORDER BY lockeduntil;

-- Dead-lettered
SELECT id, channel, failedat, retrycount, substr(payload, 1, 60) AS payload
FROM workqueue
WHERE failedat IS NOT NULL
ORDER BY failedat DESC;

-- Replay all dead-lettered messages of a given type
UPDATE workqueue
SET failedat = NULL, retrycount = 0, scheduledfor = NULL,
    lockeduntil = NULL, locktoken = NULL
WHERE channel = 'MyApp.Messages.OrderSubmitted'
  AND failedat IS NOT NULL;
```
