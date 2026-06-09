
A SQL-backed work queue library for low-load .NET systems. Processing requires polling the workqueue table. **Do not use in high-throughput systems** — see the Benchmarks section for what "low load" means.

For higher throughput, consider purpose-built brokers such as Kafka, Redis Streams, or RabbitMQ.

---

## Quick Start

The fastest path to a working message bus. Requires a running PostgreSQL instance.
For Redis, see the [Redis Backend](#redis-backend) section.

**1. Install**

```bash
dotnet add package TownSuite.WorkQueues
dotnet add package TownSuite.WorkQueues.Postgres
```

**2. Define a message**

```csharp
public class OrderSubmitted
{
    public Guid   OrderId  { get; set; }
    public string Customer { get; set; } = string.Empty;
}
```

**3. Write a consumer**

```csharp
public class OrderConsumer : IConsumer<OrderSubmitted>
{
    public Task Consume(ConsumeContext<OrderSubmitted> context)
    {
        Console.WriteLine($"Order received: {context.Message.OrderId}");
        return Task.CompletedTask;
        // Throw any exception to retry. After MaxRetries the message is dead-lettered.
    }
}
```

**4. Wire it up (Worker Service / ASP.NET Core)**

```csharp
// Program.cs
builder.Services.AddSingleton(new SqlTransportOptions
{
    ConnectionString      = "Host=localhost;Database=myapp;Username=app;Password=secret",
    AdminConnectionString = "Host=localhost;Database=myapp;Username=admin;Password=secret",
    Schema     = "transport",
    MaxRetries = 3
});

// Creates the workqueue table and stored procedures on first startup.
builder.Services.AddPostgresMigrationHostedService();

// Bus singleton — subscribe all consumers here before the bus starts.
builder.Services.AddSingleton<IMessageBus>(sp =>
{
    var bus = new PostgresMessageBus(
        sp.GetRequiredService<SqlTransportOptions>(),
        sp.GetRequiredService<ILogger<PostgresMessageBus>>());

    bus.Subscribe(new OrderConsumer());
    return bus;
});

// Resolves the bus after migrations complete and disposes it on shutdown.
builder.Services.AddHostedService<MessageBusHostedService>();
```

`MessageBusHostedService` is a small wrapper you add once to every project — see
[WORKER_SERVICES.md](WORKER_SERVICES.md) for the full implementation (10 lines).

**5. Publish a message**

```csharp
// From a controller, minimal API endpoint, or any service with IMessageBus injected:
await bus.Publish(new OrderSubmitted
{
    OrderId  = Guid.NewGuid(),
    Customer = "alice@example.com"
});
```

The message is written to the database and delivered to `OrderConsumer` on the next polling
cycle (within `MaxWaitTime`, default 5 s).

---

## Contents

- [Quick Start](#quick-start)
- [NuGet Package](#nuget-package)
- [Database Setup & Migrations](#database-setup--migrations)
- [Work Queue (direct enqueue/dequeue)](#work-queue-direct-enqueuededequeue)
- [Message Bus (publish/subscribe)](#message-bus-publishsubscribe)
- [Redis Backend](#redis-backend)
- [Dead-Letter Queue & Retries](#dead-letter-queue--retries)
- [Configuration Reference](#configuration-reference)
- [Running the Tests](#running-the-tests)
- [Upgrading from Earlier Versions](#upgrading-from-earlier-versions)
- [Benchmarks](#benchmarks)
- [Migration guide — direct WorkQueue → message bus](MIGRATING.md)
- [Worker Service & ASP.NET Core integration guide](WORKER_SERVICES.md)

---

## NuGet Package

Build in Release mode to produce a NuGet package, then reference it from your local feed:

```powershell
dotnet add package "TownSuite.WorkQueues" --source "C:\path\to\package\folder"
```

---

## Database Setup & Migrations

### PostgreSQL — automatic migrations on startup

Register the hosted service in your DI container. It creates the schema, table, stored procedures, and index on first run, and is safe to run on every subsequent startup.

```cs
// Program.cs / Startup.cs
builder.Services.AddSingleton(new SqlTransportOptions
{
    ConnectionString      = "Host=...;Database=mydb;Username=app;Password=...",
    AdminConnectionString = "Host=...;Database=mydb;Username=admin;Password=...",
    Schema                = "transport",   // default
    MaxBatchSize          = 100,           // default
    MaxWaitTime           = TimeSpan.FromSeconds(5), // default
    MaxRetries            = 3              // default — messages exceeding this are dead-lettered
});

builder.Services.AddPostgresMigrationHostedService();
```

### SQL Server — manual scripts

Run the scripts in `scripts/sql-server/` in this order against your database:

1. `dbo.WorkQueue.sql`
2. `dbo.WorkQueue_Enqueue.sql`
3. `dbo.WorkQueue_Dequeue.sql`
4. `dbo.WorkQueue_Dequeue_NonDestructive.sql`

---

## Work Queue (direct enqueue/dequeue)

Both PostgreSQL and SQL Server are supported. Inject `IWorkQueue` and use any open `DbConnection`.

### Enqueue

```cs
// txn is optional; pass null to auto-commit immediately
await workQueue.Enqueue("orders", new OrderPayload { Id = 42 }, cn, txn: null);
```

### Dequeue — destructive (record deleted on retrieval)

```cs
using var txn = cn.BeginTransaction();

var order = await workQueue.Dequeue<OrderPayload>("orders", cn, txn);
if (order is null)
{
    txn.Rollback();
    return; // queue empty
}

// process order ...
txn.Commit();
```

### Dequeue — non-destructive (record kept, marked with timeprocessedutc)

```cs
var workQueue = new DbBackedWorkQueue_NonDestructive();

using var txn = cn.BeginTransaction();
var order = await workQueue.Dequeue<OrderPayload>("orders", cn, txn);
txn.Commit();
```

### Skipping failed records with offset

The `offset` parameter lets multiple workers share a channel without blocking on each other, or lets a single worker skip a problematic record and come back to it later:

```cs
int offset = 0;
while (true)
{
    using var txn = cn.BeginTransaction();
    var item = await workQueue.Dequeue<Payload>("channel", cn, txn, offset);

    if (item is null) break;

    try
    {
        Process(item);
        txn.Commit();
        offset = 0; // reset on success
    }
    catch (Exception ex)
    {
        txn.Rollback();
        logger.LogError(ex, "Failed at offset {Offset}", offset);
        offset++;   // skip this record next iteration
    }
}
```

> **Channel name limit:** channel names must not exceed 500 characters. The channel column is `VARCHAR(500)` in both databases.

---

## Message Bus (publish/subscribe)

`PostgresMessageBus` provides a lightweight pub/sub layer backed by the same workqueue table. A background polling loop claims messages using `FOR UPDATE SKIP LOCKED` and dispatches them to in-memory consumers.

### Consumers

```cs
public class OrderConsumer : IConsumer<OrderSubmitted>
{
    public async Task Consume(ConsumeContext<OrderSubmitted> context)
    {
        var order = context.Message;
        Console.WriteLine($"Processing order {order.OrderId}");
        await Task.CompletedTask;
    }
}
```

### Wiring up the bus (standalone / console)

```cs
var options = new SqlTransportOptions
{
    ConnectionString      = connectionString,
    AdminConnectionString = connectionString,
    Schema                = "transport",
    MaxBatchSize          = 50,
    MaxWaitTime           = TimeSpan.FromSeconds(2),
    MaxRetries            = 3
};

using var bus = new PostgresMessageBus(options, logger);

bus.Subscribe(new OrderConsumer());

await bus.Publish(new OrderSubmitted
{
    OrderId     = Guid.NewGuid(),
    ProductName = "Widget"
});
```

Multiple consumers can be subscribed to the same message type; each receives a copy of every message.

> **Worker Service / ASP.NET Core:** see the [Worker Services integration guide](WORKER_SERVICES.md) for complete `Program.cs` examples, scoped DI patterns, graceful shutdown, and Windows Service / systemd deployment.

### Message type names as channels

The message bus derives the channel name from `typeof(T).FullName`. Keep message type names under 500 characters (straightforward for any normal namespace depth).

---

## Redis Backend

`TownSuite.WorkQueues.Redis` provides two Redis-backed implementations that do not require a database:

| Type | Interface | Backing structure |
|---|---|---|
| `RedisWorkQueue` | `IRedisWorkQueue` | Redis List (LPUSH / RPOP) |
| `RedisMessageBus` | `IMessageBus` | Redis Streams (XADD / XREADGROUP / XAUTOCLAIM) |

### Installation

```powershell
dotnet add package TownSuite.WorkQueues.Redis
```

### Redis work queue

```cs
using var mux = ConnectionMultiplexer.Connect("localhost:6379");
var queue = new RedisWorkQueue(mux, new RedisOptions { KeyPrefix = "myapp" });

// Enqueue
await queue.EnqueueAsync("orders", new OrderPayload { Id = 42 });

// Dequeue (returns null when empty)
var item = await queue.DequeueAsync<OrderPayload>("orders");
```

FIFO order is guaranteed per channel. There is no retry or dead-letter logic in `RedisWorkQueue` — use `RedisMessageBus` when you need those.

### Redis message bus

The same `IConsumer<T>` and `IMessageBus` contracts used by `PostgresMessageBus` apply here.

```cs
using var mux = ConnectionMultiplexer.Connect("localhost:6379");

var options = new RedisOptions
{
    KeyPrefix     = "myapp",
    ConsumerGroup = "workers",
    MaxBatchSize  = 50,
    MaxWaitTime   = TimeSpan.FromSeconds(2),
    MaxRetries    = 3
};

using var bus = new RedisMessageBus(mux, options, logger);
bus.Subscribe(new OrderConsumer());

await bus.Publish(new OrderSubmitted { OrderId = Guid.NewGuid() });
```

#### How retry and dead-letter work

1. A message is claimed with `XREADGROUP`. On failure, it stays in the Pending Entry List.
2. Once idle for `ReclaimIdleTime` (default `MaxWaitTime × 3`), `XAUTOCLAIM` reclaims it and increments its retry counter.
3. When `retryCount >= MaxRetries`, the message is copied to `{prefix}:stream:{type}:dead` and ACK-ed on the main stream.

Dead-lettered messages can be inspected and replayed using `XREAD` or any Redis client.

### DI registration

```cs
builder.Services
    .AddRedisConnection("localhost:6379")
    .AddRedisMessageBus(opts =>
    {
        opts.KeyPrefix     = "myapp";
        opts.ConsumerGroup = "workers";
        opts.MaxRetries    = 3;
    });

// Optionally register a work queue on the same connection
builder.Services.AddRedisWorkQueue(opts => opts.KeyPrefix = "myapp");
```

`Subscribe` calls must be made before the application starts accepting traffic so the polling loop sees the handlers.

> **Worker Service / ASP.NET Core:** see the [Worker Services integration guide](WORKER_SERVICES.md) for complete Redis `Program.cs` examples, scoped DI, graceful shutdown, and deployment.

### `RedisOptions` reference

| Property | Default | Description |
|---|---|---|
| `KeyPrefix` | `"workqueue"` | Prefix for all Redis keys |
| `ConsumerGroup` | `"default"` | Stream consumer group name |
| `ConsumerName` | `Environment.MachineName` | Unique identity within the group |
| `ReclaimIdleTime` | `MaxWaitTime × 3` | Idle threshold before a pending message is reclaimed |
| `MaxBatchSize` | `100` (inherited) | Messages read per polling cycle |
| `MaxWaitTime` | `5s` (inherited) | Pause when the stream is empty |
| `MaxRetries` | `3` (inherited) | Attempts before dead-lettering |

---

## Dead-Letter Queue & Retries

When a consumer throws, the message is **not** marked as processed. Instead:

1. `retrycount` is incremented for that row.
2. On the next polling cycle the message is picked up and retried.
3. Once `retrycount >= MaxRetries` (default `3`), the row's `failedat` column is set to the current timestamp and the message is permanently excluded from polling.

Dead-lettered rows (`failedat IS NOT NULL`) remain in the table for inspection and manual replay. No separate table is required.

```sql
-- inspect failed messages
SELECT * FROM transport.workqueue WHERE failedat IS NOT NULL ORDER BY failedat DESC;

-- manually replay a failed message (clears dead-letter state)
UPDATE transport.workqueue
SET failedat = NULL, retrycount = 0
WHERE id = 42;
```

---

## Configuration Reference

### `BatchOptions`

| Property | Default | Description |
|---|---|---|
| `MaxBatchSize` | `100` | Maximum messages claimed per polling cycle |
| `MaxWaitTime` | `5s` | How long to pause when the queue is empty before polling again |
| `AllowEmptyBatches` | `false` | Reserved for future use |
| `MaxRetries` | `3` | Delivery attempts before a message is dead-lettered |

### `SqlTransportOptions` (extends `BatchOptions`)

| Property | Default | Description |
|---|---|---|
| `ConnectionString` | — | Connection string used for message reads and writes |
| `AdminConnectionString` | — | Connection string used for running migrations (may need DDL permissions) |
| `Schema` | `"transport"` | PostgreSQL schema that contains the workqueue table |

---

## Running the Tests

Tests use [Testcontainers](https://dotnet.testcontainers.org/) and spin up real PostgreSQL and SQL Server instances automatically. The only prerequisite is a running Docker daemon.

```bash
cd TownSuite.WorkQueues.Testing
dotnet test
```

No external database setup or `appsettings.json` changes are needed.

---

## Upgrading from Earlier Versions

### Serialization change (breaking)

Prior to the production-ready update, `DbBackedWorkQueue` serialised payloads using Newtonsoft.Json with `TypeNameHandling.All`, embedding `$type` metadata and indented formatting:

```json
{
  "$type": "MyApp.OrderPayload, MyApp",
  "Id": 42
}
```

The library now uses `System.Text.Json` with no type metadata:

```json
{"Id":42}
```

**Action required before upgrading:** drain the queue so no messages serialised by the old code remain. Messages written by the old code cannot be deserialised by the new code.

### Schema changes

The following DDL changes are applied automatically by `PostgresMigrationHostedService` on first startup after upgrade. For SQL Server, apply the corresponding alterations manually.

| Change | Detail |
|---|---|
| `channel` column widened | `VARCHAR(50)` → `VARCHAR(500)` |
| `retrycount` column added | `INT NOT NULL DEFAULT 0` |
| `failedat` column added | `TIMESTAMP NULL` |
| Partial index added | On `(channel, timecreatedutc)` where unprocessed and not failed |

The migration statements are idempotent (`IF NOT EXISTS`, `ADD COLUMN IF NOT EXISTS`, `DO` block guards) and safe to run on an existing table with live data.

---

## Benchmarks

These numbers are from a single client machine talking to a dedicated database host. The library is designed for **sustained throughput below 10,000 calls/second**.

### PostgreSQL

| Threads | Calls/1 s | Calls/30 s |
|---|---|---|
| 1 | 1,953 | 58,590 |
| 10 | 11,509 | 345,278 |
| 20 | 16,661 | 499,844 |
| 30 | 18,062 | 541,895 |
| 40 | 18,366 | 551,004 |
| 50 | 19,090 | 572,727 |

```ini
BenchmarkDotNet=v0.13.5, OS=macOS Ventura 13.4.1 [Darwin 22.5.0]
Apple M1 Max, 1 CPU, 10 logical and 10 physical cores
.NET SDK=6.0.408 / .NET 6.0.16, Arm64 RyuJIT AdvSIMD
```

| Method | Mean | Error | StdDev |
|---|---|---|---|
| Enqueue | 519.1 µs | 15.16 µs | 44.47 µs |

### SQL Server

| Threads | Calls/1 s | Calls/30 s |
|---|---|---|
| 1 | 1,241 | 37,228 |
| 10 | 12,606 | 378,272 |
| 20 | 12,943 | 388,693 |
| 30 | 13,135 | 394,284 |
| 40 | 13,210 | 396,515 |
| 50 | 12,944 | 388,545 |

```ini
BenchmarkDotNet=v0.13.5, OS=macOS Ventura 13.4.1 [Darwin 22.5.0]
Apple M1 Max, 1 CPU, 10 logical and 10 physical cores
.NET SDK=6.0.408 / .NET 6.0.16, Arm64 RyuJIT AdvSIMD
```

| Method | Mean | Error | StdDev |
|---|---|---|---|
| Enqueue | 794.8 µs | 12.84 µs | 12.01 µs |
