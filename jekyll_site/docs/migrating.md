---
layout: docs
title: Migrating from v1 — TownSuite.WorkQueues
description: Step-by-step guide for upgrading TownSuite.WorkQueues from version 1.x to version 2.x.
permalink: /docs/migrating/
---

# Migrating from v1 to v2

Version 2 is a substantial overhaul of the library. Most of the changes are additive, but there are three things that require action on upgrade: a **serialization format change**, **schema changes**, and the **removal of Newtonsoft.Json**.

---

## At a glance

| Area | Action required |
|---|---|
| NuGet packages | Add backend-specific packages; add explicit Newtonsoft.Json dep if needed |
| Serialization | No action for POCOs; drain queue if using polymorphic payloads |
| Schema | Run `AddPostgresMigrationHostedService()` or apply SQL manually |
| `AllowEmptyBatches` option | Rename to `ContinuousPolling` (old name compiles with a warning) |
| Dead-letter queue | New — set `MaxRetries` (default `3`) |
| Everything else | Non-breaking additions — opt in at your own pace |

---

## 1. Update packages

The core package is still `TownSuite.WorkQueues`. Backend packages are now separate:

```sh
dotnet add package TownSuite.WorkQueues
dotnet add package TownSuite.WorkQueues.Postgres  # or .SqlServer, .Redis, .Sqlite
```

**Newtonsoft.Json is no longer a transitive dependency.** If your project relied on it transitively, add an explicit reference:

```sh
dotnet add package Newtonsoft.Json
```

---

## 2. Serialization change

v2 writes payloads with `System.Text.Json`. v1 used `Newtonsoft.Json` with `TypeNameHandling.All`, which embeds `$type` metadata:

```json
// v1 payload
{"$type":"MyApp.OrderSubmitted, MyApp","OrderId":"...","Customer":"alice"}

// v2 payload
{"OrderId":"...","Customer":"alice"}
```

**Reading old v1 payloads is handled automatically.** The library detects and strips `$type` annotations on POCOs (including nested objects) and unwraps collections stored in `$values` arrays. No drain is required for the common case.

The one scenario that still requires action is **polymorphic payloads** — where a derived type was stored (`$type` pointed to `OrderSubmittedV2`) but the consumer reads it as a base type (`OrderEvent`). In that case the derived-only properties are silently dropped. Options:

- Drain the affected channel before deploying v2 (let all existing messages process first).
- Or, after deploying, call `ReplayDeadLettered<T>()` if any messages land in the dead-letter queue due to deserialization errors.

For all other cases — simple POCOs, records, collections — upgrade in-place without draining.

---

## 3. Schema migration

v2 adds three new columns and a partial index to the `workqueue` table:

| Change | Detail |
|---|---|
| `channel` column widened | `VARCHAR(50)` → `VARCHAR(500)` |
| `retrycount` column added | `INT NOT NULL DEFAULT 0` |
| `failedat` column added | `TIMESTAMP NULL` (Postgres) / `DATETIME NULL` (SQL Server) |
| Partial index added | `(channel, timecreatedutc) WHERE timeprocessedutc IS NULL AND failedat IS NULL` |

v2.4 adds two more columns required for scheduled delivery and message identity:

| Change | Detail |
|---|---|
| `scheduledfor` column added | `TIMESTAMP NULL` / `DATETIME NULL` |
| `messageid` column added | `UUID NOT NULL DEFAULT gen_random_uuid()` / `UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID()` |

### Automatic migration (recommended)

Register the migration hosted service and all DDL is applied on startup. Every statement is idempotent and safe to run against a live database:

```csharp
// PostgreSQL
builder.Services.AddPostgresMigrationHostedService();

// SQL Server
builder.Services.AddSqlServerMigrationHostedService();
```

### Manual migration (PostgreSQL)

If you manage schema separately, run these statements in order:

```sql
-- Widen channel column
ALTER TABLE transport.workqueue
    ALTER COLUMN channel TYPE VARCHAR(500);

-- Add retrycount
ALTER TABLE transport.workqueue
    ADD COLUMN IF NOT EXISTS retrycount INT NOT NULL DEFAULT 0;

-- Add failedat
ALTER TABLE transport.workqueue
    ADD COLUMN IF NOT EXISTS failedat TIMESTAMP NULL;

-- Add partial index (v2.0)
CREATE INDEX IF NOT EXISTS ix_workqueue_channel_unprocessed
    ON transport.workqueue (channel, timecreatedutc)
    WHERE timeprocessedutc IS NULL AND failedat IS NULL;

-- Add scheduledfor (v2.4)
ALTER TABLE transport.workqueue
    ADD COLUMN IF NOT EXISTS scheduledfor TIMESTAMP NULL;

-- Add messageid (v2.4)
ALTER TABLE transport.workqueue
    ADD COLUMN IF NOT EXISTS messageid UUID NOT NULL DEFAULT gen_random_uuid();
```

### Manual migration (SQL Server)

```sql
-- Widen channel column
ALTER TABLE [dbo].[workqueue]
    ALTER COLUMN [channel] NVARCHAR(500) NOT NULL;

-- Add retrycount
IF NOT EXISTS (SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID('[dbo].[workqueue]') AND name = 'retrycount')
    ALTER TABLE [dbo].[workqueue]
        ADD [retrycount] INT NOT NULL DEFAULT 0;

-- Add failedat
IF NOT EXISTS (SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID('[dbo].[workqueue]') AND name = 'failedat')
    ALTER TABLE [dbo].[workqueue]
        ADD [failedat] DATETIME NULL;

-- Add partial index (v2.0)
IF NOT EXISTS (SELECT 1 FROM sys.indexes
    WHERE name = 'IX_WorkQueue_Channel_Unprocessed'
    AND object_id = OBJECT_ID('[dbo].[workqueue]'))
    CREATE NONCLUSTERED INDEX [IX_WorkQueue_Channel_Unprocessed]
        ON [dbo].[workqueue] ([channel] ASC, [timecreatedutc] ASC)
        WHERE [timeprocessedutc] IS NULL AND [failedat] IS NULL;

-- Add scheduledfor (v2.4)
IF NOT EXISTS (SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID('[dbo].[workqueue]') AND name = 'scheduledfor')
    ALTER TABLE [dbo].[workqueue]
        ADD [scheduledfor] DATETIME NULL;

-- Add messageid (v2.4)
IF NOT EXISTS (SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID('[dbo].[workqueue]') AND name = 'messageid')
    ALTER TABLE [dbo].[workqueue]
        ADD [messageid] UNIQUEIDENTIFIER NOT NULL
            CONSTRAINT [DEFAULT_WorkQueue_MessageId] DEFAULT (NEWID()) WITH VALUES;
```

---

## 4. API changes

### `AllowEmptyBatches` renamed to `ContinuousPolling`

The property was renamed to better describe its behaviour. The old name still compiles but emits an `[Obsolete]` warning:

```csharp
// v1
options.AllowEmptyBatches = true;

// v2
options.ContinuousPolling = true;
```

### Dead-letter queue is now automatic

v2 introduces `MaxRetries` (default `3`). If a consumer throws on every attempt, the message is automatically dead-lettered after three tries — `failedat` is set and polling skips it permanently. In v1 there was no retry limit and failed messages would retry forever.

If you relied on that unlimited-retry behaviour, raise `MaxRetries` to a large number:

```csharp
options.MaxRetries = int.MaxValue;  // effectively unlimited, v1 behaviour
```

To inspect and replay dead-lettered messages:

```csharp
// Programmatic replay
int replayed = await bus.ReplayDeadLettered<OrderSubmitted>();

// Or via SQL (PostgreSQL)
UPDATE transport.workqueue
SET failedat = NULL, retrycount = 0
WHERE failedat IS NOT NULL
  AND channel = 'MyApp.OrderSubmitted';
```

### Security: `TypeNameHandling.All` removed

v1 used `Newtonsoft.Json` with `TypeNameHandling.All`, which allows arbitrary type instantiation from attacker-controlled JSON — a known deserialization vulnerability. v2 uses `System.Text.Json` with no type metadata. No action needed; this is strictly a security improvement.

---

## 5. New features to adopt

These are all additive and opt-in. There is no pressure to adopt them immediately.

**`ConsumeContext<T>` now carries metadata** (v2.3 / v2.4):

```csharp
public async Task Consume(ConsumeContext<OrderSubmitted> ctx)
{
    // ctx.CancellationToken — observe graceful shutdown
    // ctx.MessageId         — stable UUID; use for idempotency checks
    // ctx.SentTime          — when Publish() was called
    if (await _idempotency.IsProcessed(ctx.MessageId)) return;
    await ProcessOrder(ctx.Message, ctx.CancellationToken);
}
```

**Scoped consumers** — resolved from a fresh `IServiceScope` per message (v2.4):

```csharp
services.AddScoped<OrderConsumer>();
services.AddPostgresMessageBus((sp, bus) =>
{
    bus.Subscribe<OrderSubmitted, OrderConsumer>();
});
```

**Scheduled delivery** (v2.4):

```csharp
await bus.Publish(new ReminderEmail { UserId = userId },
    deliverAfter: DateTimeOffset.UtcNow.AddHours(2));
```

**Fault consumers** — notified when a message is dead-lettered (v2.4):

```csharp
bus.SubscribeFault<OrderSubmitted>(new OrderAlertConsumer());
```

**`IsPolling` health check** (v2.3):

```csharp
builder.Services.AddHealthChecks()
    .AddCheck("message-bus", () =>
        sp.GetRequiredService<IMessageBus>().IsPolling
            ? HealthCheckResult.Healthy()
            : HealthCheckResult.Unhealthy("Bus polling loop has stopped"));
```

**Retry delay / backoff** (v2.4):

```csharp
options.RetryDelay = TimeSpan.FromSeconds(30);  // hold failed messages 30 s before retry
```
