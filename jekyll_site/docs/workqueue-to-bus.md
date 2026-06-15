---
layout: docs
title: WorkQueue → Message Bus — TownSuite.WorkQueues
description: How to migrate from IWorkQueue / DbBackedWorkQueue to IMessageBus and IConsumer<T>.
permalink: /docs/workqueue-to-bus/
---

# WorkQueue → Message Bus

This guide covers migrating from the pull-based `IWorkQueue` / `DbBackedWorkQueue` pattern — where your code owns the polling loop, connection, and transaction — to the push-based `IMessageBus` / `IConsumer<T>` pattern, where the library owns all of that.

---

## Should you migrate?

Work through these questions before starting. Some patterns don't belong in the bus at all.

| Question | If yes | If no |
|---|---|---|
| Is your database PostgreSQL or SQL Server? | Continue | Stop — stay with `IWorkQueue` or switch to `RedisMessageBus` |
| Does your dequeue run inside a transaction that also writes to other tables? | **Stay with `IWorkQueue`** — the bus commits internally; you cannot join its transaction | Continue |
| Do you use `DbBackedWorkQueue_NonDestructive` and re-read processed rows for audit? | **Stay** — the bus marks rows processed and excludes them from future polls | Continue |
| Do you rely on the `offset` parameter to skip bad messages across workers? | The bus handles this automatically via retries and dead-lettering. Continue. | Continue |
| Are you happy for the channel name to be `typeof(T).FullName`? | Continue | Read the [channel name](#channel-name) section carefully before deciding |

<div class="callout">
<strong>Do not migrate</strong> any call site where <code>Dequeue</code> runs inside the same transaction as business-critical writes to other tables. The message bus manages its own transaction and you cannot enlist it in yours.
</div>

---

## What changes

Three things shift conceptually.

### Pull → push

```
IWorkQueue (pull)                      IMessageBus (push)
───────────────────────────────────    ───────────────────────────────────
You write the loop                     Library owns the loop
You open the connection                Library opens the connection
You manage the transaction             Library manages the transaction
You handle retry / skip logic          Library retries, then dead-letters
```

### Channel name

This is the most common migration pitfall.

| | Channel value |
|---|---|
| `workQueue.Enqueue("orders", ...)` | `"orders"` — whatever string you pass |
| `bus.Publish<OrderPayload>(...)` | `"MyApp.Messages.OrderPayload"` — `typeof(T).FullName`, auto-derived |

Messages already sitting in the database under `"orders"` will **not** be picked up by a bus subscriber for `OrderPayload`, because the bus polls for `"MyApp.Messages.OrderPayload"`. See [handling in-flight messages](#handling-in-flight-messages).

### Transaction ownership

```csharp
// IWorkQueue — you own the transaction
using var txn = cn.BeginTransaction();
var item = await workQueue.Dequeue<OrderPayload>("orders", cn, txn);
if (item is null) { txn.Rollback(); return; }
await ProcessOrder(item);
txn.Commit(); // or Rollback() on failure — your decision

// IMessageBus — the library owns the transaction
public class OrderConsumer : IConsumer<OrderPayload>
{
    public async Task Consume(ConsumeContext<OrderPayload> ctx)
    {
        await ProcessOrder(ctx.Message);
        // Throw to retry. After MaxRetries the message is dead-lettered.
        // No transaction to open or commit.
    }
}
```

---

## Step-by-step migration

### 1. Find all call sites

Search the codebase for:

```
IWorkQueue
DbBackedWorkQueue
.Enqueue(
.Dequeue<
```

For each match, note the channel string and the payload type `T`.

### 2. Create a named message type

The bus derives the channel from `typeof(T).FullName`. The type must be a named class or record — it cannot be an anonymous type or `Dictionary<string, object>`.

```csharp
// Before — anonymous type, no stable channel name
await workQueue.Enqueue("orders", new { OrderId = 42, Customer = "Alice" }, cn);

// After — named record, channel = "MyApp.Messages.OrderPayload"
public record OrderPayload(int OrderId, string Customer);
```

Keep the class name and namespace stable after migration. Moving or renaming the type changes the channel name and orphans any messages stored under the old name.

### 3. Replace the polling worker with IConsumer&lt;T&gt;

Take the processing logic from inside the dequeue loop and put it in `Consume`. Remove all connection, transaction, null-check, and offset management.

**Before:**

```csharp
public class OrderWorker : BackgroundService
{
    private readonly IWorkQueue _queue;
    private readonly NpgsqlDataSource _db;

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            await using var cn = await _db.OpenConnectionAsync(ct);
            await using var txn = await cn.BeginTransactionAsync(ct);

            var item = await _queue.Dequeue<OrderPayload>("orders", cn, txn);
            if (item is null)
            {
                await txn.RollbackAsync(ct);
                await Task.Delay(TimeSpan.FromSeconds(5), ct);
                continue;
            }

            try
            {
                await ProcessOrder(item, ct);
                await txn.CommitAsync(ct);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to process order {Id}", item.OrderId);
                await txn.RollbackAsync(ct);
                // message returns to queue on rollback; retried next cycle
            }
        }
    }
}
```

**After:**

```csharp
public class OrderConsumer : IConsumer<OrderPayload>
{
    private readonly ILogger<OrderConsumer> _logger;

    public OrderConsumer(ILogger<OrderConsumer> logger) => _logger = logger;

    public async Task Consume(ConsumeContext<OrderPayload> ctx)
    {
        await ProcessOrder(ctx.Message, ctx.CancellationToken);
        // throw → retry; return normally → acknowledged
    }
}
```

Delete `OrderWorker` entirely. The bus provides the loop, connection, and retry logic.

### 4. Replace Enqueue with Publish

```csharp
// Before — requires an open connection and optional transaction
await _workQueue.Enqueue("orders", payload, cn, txn);

// After — no connection or transaction arguments
await _bus.Publish(payload);
```

`Publish` opens its own connection internally. If you need to publish inside a business transaction, see [transactional publishing](#transactional-publishing) below.

### 5. Register in DI

```csharp
// Program.cs

// Options (replace with SqlServerTransportOptions for SQL Server)
builder.Services.AddSingleton(new SqlTransportOptions
{
    ConnectionString = "Host=localhost;Database=myapp;Username=app;Password=secret",
    Schema     = "transport",
    MaxRetries = 3,
    RetryDelay = TimeSpan.FromSeconds(10)   // optional back-off between retries
});

// Migration — creates / updates table on startup
builder.Services.AddPostgresMigrationHostedService();

// Bus — register all consumers inside the configure callback
builder.Services.AddPostgresMessageBus((sp, bus) =>
{
    // Singleton consumer (stateless)
    bus.Subscribe(new OrderConsumer(sp.GetRequiredService<ILogger<OrderConsumer>>()));

    // Or DI-scoped — a fresh instance is resolved per message
    // bus.Subscribe<OrderPayload, OrderConsumer>();
});
```

### 6. Remove the old worker

```csharp
// Remove this
builder.Services.AddHostedService<OrderWorker>();
// and the IWorkQueue registration if nothing else uses it
```

### 7. Handle in-flight messages

Before deploying, decide what to do with messages still sitting in the database under the old channel name (e.g. `"orders"`).

**Option A — drain first (cleanest):** wait for the old worker to process everything under `"orders"`, then deploy the new code.

**Option B — rename via SQL:** bulk-update the channel column before deploying so the new bus sees them immediately.

```sql
-- PostgreSQL
UPDATE transport.workqueue
SET channel = 'MyApp.Messages.OrderPayload'
WHERE channel = 'orders'
  AND timeprocessedutc IS NULL
  AND failedat IS NULL;

-- SQL Server
UPDATE [dbo].[workqueue]
SET [channel] = 'MyApp.Messages.OrderPayload'
WHERE [channel] = 'orders'
  AND [timeprocessedutc] IS NULL
  AND [failedat] IS NULL;
```

**Option C — parallel cutover:** deploy both the old worker and the new bus simultaneously. The old worker drains rows under `"orders"`; the new bus handles newly published rows under `"MyApp.Messages.OrderPayload"`. Remove the old worker once its channel is empty.

---

## Handling in-flight messages

| Strategy | How | When to use |
|---|---|---|
| Drain then deploy | Old worker finishes; then deploy | Low-traffic queues, maintenance window available |
| SQL channel rename | `UPDATE ... SET channel = 'FullTypeName'` before deploy | Few rows; can tolerate a brief manual step |
| Parallel cutover | Old worker + new bus run side by side | Cannot afford downtime; high-traffic queues |
| Keep old channel | Keep `IWorkQueue` producer; migrate only the consumer later | Gradual rollout across multiple deploys |

---

## Transactional publishing

`IMessageBus.Publish` always opens its own connection. If you need the publish to be atomic with other database writes, you cannot pass a transaction to it.

The cleanest solution is to keep `IWorkQueue.Enqueue` as a transactional outbox — write to the queue inside your business transaction, and let the bus poll and deliver:

```csharp
// Service method — runs inside a business transaction
public async Task SubmitOrderAsync(OrderPayload payload, DbConnection cn, DbTransaction txn)
{
    // Update your business data
    await _orderRepository.InsertAsync(payload, cn, txn);

    // Write to the queue transactionally.
    // Use the type's FullName as the channel — the bus polls for exactly this string.
    await _workQueue.Enqueue(
        channel: typeof(OrderPayload).FullName!,
        payload: payload,
        con: cn,
        txn: txn);

    await txn.CommitAsync();
    // If this commit fails, both the order insert and the queue row are rolled back.
}
```

The bus polls for `"MyApp.Messages.OrderPayload"` (the FullName), finds these rows, and delivers them to `OrderConsumer` — no outbox table or change-data-capture pipeline needed.

---

## Dead-letter management

After migration, failed messages accumulate with `failedat IS NOT NULL`:

```sql
-- Inspect dead-lettered messages
SELECT id, channel, retrycount, failedat,
       substr(payload, 1, 80) AS payload_preview
FROM transport.workqueue
WHERE failedat IS NOT NULL
ORDER BY failedat DESC;

-- Replay programmatically
int replayed = await bus.ReplayDeadLettered<OrderPayload>();

-- Or via SQL
UPDATE transport.workqueue
SET failedat = NULL, retrycount = 0, scheduledfor = NULL
WHERE channel = 'MyApp.Messages.OrderPayload'
  AND failedat IS NOT NULL;
```

To receive a notification the moment a message is dead-lettered:

```csharp
bus.SubscribeFault<OrderPayload>(new OrderDeadLetterHandler());

class OrderDeadLetterHandler : IConsumer<Fault<OrderPayload>>
{
    public Task Consume(ConsumeContext<Fault<OrderPayload>> ctx)
    {
        var f = ctx.Message;
        // f.OriginalMessage, f.ExceptionType, f.AttemptCount, f.FaultedAt
        return AlertOpsTeam(f);
    }
}
```

---

## API comparison

| Concern | `IWorkQueue` | `IMessageBus` |
|---|---|---|
| Enqueue / publish | `workQueue.Enqueue("channel", obj, cn, txn)` | `bus.Publish(obj)` |
| Dequeue / consume | `workQueue.Dequeue<T>("channel", cn, txn)` | `IConsumer<T>.Consume(ctx)` |
| Channel name | Any string you supply | `typeof(T).FullName` (automatic) |
| Connection | Caller provides open `DbConnection` | Library manages internally |
| Transaction | Caller provides `DbTransaction` | Library manages internally |
| Polling loop | You write it (`BackgroundService`) | Built into the bus |
| Retry on failure | Rollback + loop again (or skip with `offset`) | Automatic up to `MaxRetries` |
| Dead-letter | None — you decide what to do on failure | `failedat` column; queryable; replayable |
| Multiple consumers for same message | Not applicable (one dequeuer per item) | Multiple `Subscribe` calls; each gets a copy |
| Delivery order | FIFO by `id` | FIFO by `timecreatedutc` |
| Supports scheduling | No | `bus.Publish(msg, deliverAfter: ...)` |
| Health check | Write your own | `bus.IsPolling` |
| Stored procedures | `workqueue_enqueue` / `workqueue_dequeue` | None — direct SQL |
| Backends | PostgreSQL, SQL Server | PostgreSQL, SQL Server, Redis, SQLite |
