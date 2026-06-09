# Using TownSuite.WorkQueues with Worker Services

This guide shows how to integrate the message bus and work queue into a .NET Worker Service
(background process) or an ASP.NET Core web application that also runs background consumers.
It is written for both humans and AI coding assistants.

---

## Contents

- [Concepts and lifecycle](#concepts-and-lifecycle)
- [PostgreSQL — Worker Service](#postgresql--worker-service)
- [Redis — Worker Service](#redis--worker-service)
- [ASP.NET Core — web + background in one process](#aspnet-core--web--background-in-one-process)
- [Consumers that need scoped services](#consumers-that-need-scoped-services)
- [Multiple message types](#multiple-message-types)
- [Publishing from anywhere in the application](#publishing-from-anywhere-in-the-application)
- [Configuration via appsettings.json](#configuration-via-appsettingsjson)
- [Deploying as a Windows Service or systemd unit](#deploying-as-a-windows-service-or-systemd-unit)
- [Checklist for AI coding assistants](#checklist-for-ai-coding-assistants)

---

## Concepts and lifecycle

### How the bus fits into the hosted service model

Both `PostgresMessageBus` and `RedisMessageBus` start a background polling loop the moment
they are constructed. To integrate cleanly with ASP.NET Core's hosted service lifecycle:

1. **Delay construction** — resolve the bus singleton inside a `IHostedService.StartAsync`,
   not in a constructor or at registration time. This guarantees the bus starts *after* the
   migrations hosted service has finished running.

2. **Stop on shutdown** — both buses implement `IDisposable`. Calling `Dispose` cancels the
   polling loop and waits up to 10 seconds for in-flight dispatches to complete.

The pattern below wraps both concerns in a single `MessageBusHostedService` that every
example in this guide reuses.

```csharp
// Infrastructure/MessageBusHostedService.cs
internal sealed class MessageBusHostedService : IHostedService
{
    private readonly IServiceProvider _sp;
    private IMessageBus? _bus;

    public MessageBusHostedService(IServiceProvider sp) => _sp = sp;

    public Task StartAsync(CancellationToken cancellationToken)
    {
        // Resolve here (not in the constructor) so that any migration hosted
        // service registered before this one has already completed.
        _bus = _sp.GetRequiredService<IMessageBus>();
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        (_bus as IDisposable)?.Dispose();
        return Task.CompletedTask;
    }
}
```

### Registration order matters

Register services in this order so migrations run before the bus starts processing:

```
1. AddPostgresMigrationHostedService()   — runs DDL on startup
2. AddSingleton<IMessageBus>(factory)    — deferred; created only when first resolved
3. AddHostedService<MessageBusHostedService>() — resolves the bus in StartAsync
```

For Redis there is no migration step, so only steps 2 and 3 are needed.

---

## PostgreSQL — Worker Service

### Create the project

```bash
dotnet new worker -n OrderProcessor
cd OrderProcessor
dotnet add package TownSuite.WorkQueues
dotnet add package TownSuite.WorkQueues.Postgres
```

### Project structure

```
OrderProcessor/
├── appsettings.json
├── Program.cs
├── Infrastructure/
│   └── MessageBusHostedService.cs
├── Messages/
│   └── OrderSubmitted.cs
└── Consumers/
    └── OrderConsumer.cs
```

### Messages/OrderSubmitted.cs

```csharp
namespace OrderProcessor.Messages;

public class OrderSubmitted
{
    public Guid OrderId { get; set; }
    public string CustomerEmail { get; set; } = string.Empty;
    public decimal Total { get; set; }
}
```

### Consumers/OrderConsumer.cs

```csharp
using Microsoft.Extensions.Logging;
using OrderProcessor.Messages;
using TownSuite.WorkQueues;

namespace OrderProcessor.Consumers;

public class OrderConsumer : IConsumer<OrderSubmitted>
{
    private readonly ILogger<OrderConsumer> _logger;

    public OrderConsumer(ILogger<OrderConsumer> logger) => _logger = logger;

    public async Task Consume(ConsumeContext<OrderSubmitted> context)
    {
        var order = context.Message;
        _logger.LogInformation("Processing order {OrderId} for {Email} — total {Total:C}",
            order.OrderId, order.CustomerEmail, order.Total);

        // Do real work here: call APIs, write to a database, send emails, etc.
        await Task.CompletedTask;

        // Throw any exception to trigger a retry (up to MaxRetries).
        // After MaxRetries the message is dead-lettered and excluded from future polls.
    }
}
```

### Infrastructure/MessageBusHostedService.cs

```csharp
using TownSuite.WorkQueues;

namespace OrderProcessor.Infrastructure;

internal sealed class MessageBusHostedService : IHostedService
{
    private readonly IServiceProvider _sp;
    private IMessageBus? _bus;

    public MessageBusHostedService(IServiceProvider sp) => _sp = sp;

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _bus = _sp.GetRequiredService<IMessageBus>();
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        (_bus as IDisposable)?.Dispose();
        return Task.CompletedTask;
    }
}
```

### Program.cs

```csharp
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OrderProcessor.Consumers;
using OrderProcessor.Infrastructure;
using OrderProcessor.Messages;
using TownSuite.WorkQueues;
using TownSuite.WorkQueues.Postgres;

var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices((ctx, services) =>
    {
        var cfg = ctx.Configuration;

        // 1. Options
        services.AddSingleton(new SqlTransportOptions
        {
            ConnectionString      = cfg.GetConnectionString("WorkQueue")!,
            AdminConnectionString = cfg.GetConnectionString("WorkQueueAdmin")!,
            Schema                = "transport",
            MaxBatchSize          = 50,
            MaxWaitTime           = TimeSpan.FromSeconds(2),
            MaxRetries            = 3
        });

        // 2. Run DDL migrations on startup (creates the table, stored procs, index).
        //    Hosted services start in registration order, so this runs before the bus.
        services.AddPostgresMigrationHostedService();

        // 3. Bus singleton — not created yet; the factory runs lazily on first resolve.
        services.AddSingleton<IMessageBus>(sp =>
        {
            var options = sp.GetRequiredService<SqlTransportOptions>();
            var logger  = sp.GetRequiredService<ILogger<PostgresMessageBus>>();
            var bus     = new PostgresMessageBus(options, logger);

            // Register every consumer before the bus starts delivering messages.
            bus.Subscribe(sp.GetRequiredService<OrderConsumer>());

            return bus;
        });

        // 4. Register consumers so IServiceProvider can inject their dependencies.
        services.AddTransient<OrderConsumer>();

        // 5. Hosted service that resolves the bus in StartAsync (after migrations).
        services.AddHostedService<MessageBusHostedService>();
    })
    .Build();

await host.RunAsync();
```

### appsettings.json

```json
{
  "ConnectionStrings": {
    "WorkQueue":      "Host=localhost;Port=5432;Database=myapp;Username=app;Password=secret",
    "WorkQueueAdmin": "Host=localhost;Port=5432;Database=myapp;Username=admin;Password=secret"
  },
  "Logging": {
    "LogLevel": {
      "Default": "Information",
      "TownSuite": "Debug"
    }
  }
}
```

---

## Redis — Worker Service

### Create the project

```bash
dotnet new worker -n OrderProcessor
cd OrderProcessor
dotnet add package TownSuite.WorkQueues
dotnet add package TownSuite.WorkQueues.Redis
```

### Program.cs

```csharp
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OrderProcessor.Consumers;
using OrderProcessor.Infrastructure;
using StackExchange.Redis;
using TownSuite.WorkQueues;
using TownSuite.WorkQueues.Redis;

var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices((ctx, services) =>
    {
        var cfg = ctx.Configuration;

        // 1. Shared connection multiplexer (one per process — StackExchange.Redis best practice).
        services.AddSingleton<IConnectionMultiplexer>(
            _ => ConnectionMultiplexer.Connect(cfg.GetConnectionString("Redis")!));

        // 2. Bus options.
        services.AddSingleton(new RedisOptions
        {
            KeyPrefix     = "myapp",
            ConsumerGroup = "order-workers",
            MaxBatchSize  = 50,
            MaxWaitTime   = TimeSpan.FromSeconds(2),
            MaxRetries    = 3
        });

        // 3. Bus singleton.
        services.AddSingleton<IMessageBus>(sp =>
        {
            var redis   = sp.GetRequiredService<IConnectionMultiplexer>();
            var options = sp.GetRequiredService<RedisOptions>();
            var logger  = sp.GetRequiredService<ILogger<RedisMessageBus>>();
            var bus     = new RedisMessageBus(redis, options, logger);

            bus.Subscribe(sp.GetRequiredService<OrderConsumer>());

            return bus;
        });

        // 4. Register consumers.
        services.AddTransient<OrderConsumer>();

        // 5. Lifecycle management.
        services.AddHostedService<MessageBusHostedService>();
    })
    .Build();

await host.RunAsync();
```

### appsettings.json

```json
{
  "ConnectionStrings": {
    "Redis": "localhost:6379"
  }
}
```

Everything else (consumer classes, `MessageBusHostedService`) is identical to the PostgreSQL
example.

---

## ASP.NET Core — web + background in one process

A common pattern is to run an HTTP API and background consumers in the same process. The bus
is registered the same way; ASP.NET Core's hosted service infrastructure manages its lifetime.

```csharp
// Program.cs (minimal API style)
using Microsoft.Extensions.Logging;
using OrderProcessor.Consumers;
using OrderProcessor.Infrastructure;
using TownSuite.WorkQueues;
using TownSuite.WorkQueues.Postgres;

var builder = WebApplication.CreateBuilder(args);

// --- background processing setup ---

builder.Services.AddSingleton(new SqlTransportOptions
{
    ConnectionString      = builder.Configuration.GetConnectionString("WorkQueue")!,
    AdminConnectionString = builder.Configuration.GetConnectionString("WorkQueueAdmin")!,
    Schema     = "transport",
    MaxRetries = 3
});

builder.Services.AddPostgresMigrationHostedService();

builder.Services.AddSingleton<IMessageBus>(sp =>
{
    var options = sp.GetRequiredService<SqlTransportOptions>();
    var logger  = sp.GetRequiredService<ILogger<PostgresMessageBus>>();
    var bus     = new PostgresMessageBus(options, logger);

    bus.Subscribe(sp.GetRequiredService<OrderConsumer>());
    return bus;
});

builder.Services.AddTransient<OrderConsumer>();
builder.Services.AddHostedService<MessageBusHostedService>();

// --- web API setup ---

builder.Services.AddControllers();

var app = builder.Build();
app.MapControllers();

// --- publish from an HTTP endpoint ---

app.MapPost("/orders", async (OrderSubmitted order, IMessageBus bus) =>
{
    await bus.Publish(order);
    return Results.Accepted();
});

app.Run();
```

The `IMessageBus` singleton is available for injection into controllers and minimal API
handlers. `Publish` is safe to call from any thread at any time after the application starts.

---

## Consumers that need scoped services

Consumers are registered as transient/singleton and are held by the bus. If a consumer needs a
**scoped** service — such as `DbContext`, `HttpClient` (via `IHttpClientFactory`), or a
repository — inject `IServiceScopeFactory` and create a scope per message.

```csharp
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using OrderProcessor.Data;
using OrderProcessor.Messages;
using TownSuite.WorkQueues;

public class OrderConsumer : IConsumer<OrderSubmitted>
{
    private readonly IServiceScopeFactory _scopeFactory;

    public OrderConsumer(IServiceScopeFactory scopeFactory) => _scopeFactory = scopeFactory;

    public async Task Consume(ConsumeContext<OrderSubmitted> context)
    {
        // Each message gets its own DI scope — DbContext, transactions, etc.
        await using var scope = _scopeFactory.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();

        db.Orders.Add(new Order
        {
            Id    = context.Message.OrderId,
            Email = context.Message.CustomerEmail,
            Total = context.Message.Total
        });

        await db.SaveChangesAsync();
    }
}
```

Register it in `Program.cs` as a transient (or singleton — the scope inside `Consume` is what
matters, not the consumer's own lifetime):

```csharp
services.AddTransient<OrderConsumer>();
services.AddDbContext<AppDbContext>(opts =>
    opts.UseNpgsql(builder.Configuration.GetConnectionString("App")));
```

---

## Multiple message types

Register each consumer type in the bus factory and register each consumer class in DI.

```csharp
services.AddTransient<OrderConsumer>();
services.AddTransient<InvoiceConsumer>();
services.AddTransient<ShipmentConsumer>();

services.AddSingleton<IMessageBus>(sp =>
{
    var options = sp.GetRequiredService<SqlTransportOptions>();
    var logger  = sp.GetRequiredService<ILogger<PostgresMessageBus>>();
    var bus     = new PostgresMessageBus(options, logger);

    bus.Subscribe(sp.GetRequiredService<OrderConsumer>());
    bus.Subscribe(sp.GetRequiredService<InvoiceConsumer>());
    bus.Subscribe(sp.GetRequiredService<ShipmentConsumer>());

    return bus;
});
```

Multiple consumers subscribed to the **same type** each receive an independent copy of every
message. Subscriptions to **different types** share the same polling loop; the bus dispatches
all subscribed types in each cycle.

---

## Publishing from anywhere in the application

Inject `IMessageBus` directly into any class to publish. There are no connection arguments —
the bus manages its own connection internally.

```csharp
public class OrderService
{
    private readonly IMessageBus _bus;
    private readonly AppDbContext _db;

    public OrderService(IMessageBus bus, AppDbContext db)
    {
        _bus = bus;
        _db  = db;
    }

    public async Task PlaceOrderAsync(PlaceOrderRequest req)
    {
        var order = new Order { Id = Guid.NewGuid(), Email = req.Email };
        _db.Orders.Add(order);
        await _db.SaveChangesAsync();

        // Publish after the database write commits.
        // This is not transactional with the SaveChanges above.
        // For transactional publishing, see the outbox pattern in MIGRATING.md.
        await _bus.Publish(new OrderSubmitted
        {
            OrderId       = order.Id,
            CustomerEmail = order.Email,
            Total         = req.Total
        });
    }
}
```

> **Transactional publishing:** If you need "publish only if the database write commits",
> use `IWorkQueue.Enqueue` with the same open connection and transaction (outbox pattern).
> See [MIGRATING.md — Transactional publishing](MIGRATING.md#transactional-publishing).

---

## Configuration via appsettings.json

Bind options from configuration instead of hard-coding values.

### Strongly-typed options class

```csharp
// Infrastructure/WorkQueueSettings.cs
public class WorkQueueSettings
{
    public string ConnectionString      { get; set; } = string.Empty;
    public string AdminConnectionString { get; set; } = string.Empty;
    public string Schema                { get; set; } = "transport";
    public int    MaxBatchSize          { get; set; } = 50;
    public int    MaxRetries            { get; set; } = 3;
    public double MaxWaitSeconds        { get; set; } = 2;
}
```

### Program.cs binding

```csharp
var settings = builder.Configuration
    .GetSection("WorkQueue")
    .Get<WorkQueueSettings>()
    ?? throw new InvalidOperationException("WorkQueue configuration section is missing.");

builder.Services.AddSingleton(new SqlTransportOptions
{
    ConnectionString      = settings.ConnectionString,
    AdminConnectionString = settings.AdminConnectionString,
    Schema                = settings.Schema,
    MaxBatchSize          = settings.MaxBatchSize,
    MaxWaitTime           = TimeSpan.FromSeconds(settings.MaxWaitSeconds),
    MaxRetries            = settings.MaxRetries
});
```

### appsettings.json

```json
{
  "WorkQueue": {
    "ConnectionString":      "Host=db;Database=myapp;Username=app;Password=secret",
    "AdminConnectionString": "Host=db;Database=myapp;Username=admin;Password=secret",
    "Schema":                "transport",
    "MaxBatchSize":          50,
    "MaxRetries":            3,
    "MaxWaitSeconds":        2
  }
}
```

Override per environment in `appsettings.Production.json`, environment variables
(`WorkQueue__ConnectionString=...`), or Docker/Kubernetes secrets.

---

## Deploying as a Windows Service or systemd unit

### Windows Service

Add the hosting extension and publish as a single-file self-contained executable.

```bash
dotnet add package Microsoft.Extensions.Hosting.WindowsServices
```

```csharp
// Program.cs — add one line
var host = Host.CreateDefaultBuilder(args)
    .UseWindowsService()   // <-- add this
    .ConfigureServices(...)
    .Build();
```

```bash
dotnet publish -c Release -r win-x64 --self-contained -o ./publish

sc create OrderProcessor binPath= "C:\services\OrderProcessor\OrderProcessor.exe"
sc start OrderProcessor
```

### systemd (Linux)

```bash
dotnet add package Microsoft.Extensions.Hosting.Systemd
```

```csharp
var host = Host.CreateDefaultBuilder(args)
    .UseSystemd()          // <-- add this
    .ConfigureServices(...)
    .Build();
```

```bash
dotnet publish -c Release -r linux-x64 --self-contained -o /opt/order-processor
```

```ini
# /etc/systemd/system/order-processor.service
[Unit]
Description=OrderProcessor worker service
After=network.target

[Service]
WorkingDirectory=/opt/order-processor
ExecStart=/opt/order-processor/OrderProcessor
Restart=always
RestartSec=5
Environment=DOTNET_ENVIRONMENT=Production
Environment=WorkQueue__ConnectionString=Host=db;Database=myapp;Username=app;Password=secret

[Install]
WantedBy=multi-user.target
```

```bash
systemctl enable order-processor
systemctl start order-processor
journalctl -u order-processor -f   # tail logs
```

Both `UseWindowsService()` and `UseSystemd()` wire up graceful shutdown signals so
`IHostedService.StopAsync` is called correctly, giving the bus time to finish in-flight
dispatches before the process exits.

---

## Checklist for AI coding assistants

When asked to add message bus processing to a Worker Service or ASP.NET Core app:

1. **Check whether Postgres or Redis is in use.** Different packages, different option types
   (`SqlTransportOptions` vs `RedisOptions`), different connection setup.

2. **Identify all message types** the app needs to consume. Each needs an
   `IConsumer<T>` class and a `bus.Subscribe(...)` call.

3. **Check whether consumers need scoped services** (DbContext, repositories, HttpClient).
   If yes, inject `IServiceScopeFactory` and create a scope inside `Consume`, not in the
   constructor.

4. **Registration order for PostgreSQL:**
   - `AddPostgresMigrationHostedService()` first
   - `AddSingleton<IMessageBus>(factory)` second
   - `AddHostedService<MessageBusHostedService>()` third
   This ensures migrations complete before the bus starts polling.

5. **For Redis:** no migration step. Register `IConnectionMultiplexer` as a singleton from
   a connection string (one multiplexer per process). Then register `RedisOptions` and the bus.

6. **Wire `MessageBusHostedService`** — resolve `IMessageBus` inside `StartAsync`, not in
   the constructor. Dispose it in `StopAsync`.

7. **Register all consumer classes in DI** (`AddTransient<TConsumer>`) so their own
   dependencies (loggers, `IServiceScopeFactory`, etc.) are injected.

8. **Do NOT call `Subscribe` after the application has started processing messages** — add all
   subscriptions inside the factory delegate before returning the bus.

9. **For Windows Service deployment:** add `UseWindowsService()` to the host builder.
   For Linux systemd: add `UseSystemd()`. Both ensure `StopAsync` fires on shutdown signals.

10. **Configuration:** prefer `appsettings.json` + environment variable overrides over
    hard-coded connection strings. Use `IConfiguration.GetSection(...).Get<T>()` to bind.
