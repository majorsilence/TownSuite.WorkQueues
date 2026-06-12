using Dapper;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using StackExchange.Redis;
using Testcontainers.Redis;
using TownSuite.WorkQueues.Postgres;
using TownSuite.WorkQueues.Redis;
using TownSuite.WorkQueues.SqlServer;

namespace TownSuite.WorkQueues.Testing;

[TestFixture]
public class RetryAndDeadLetterTests
{
    // ── PostgreSQL ────────────────────────────────────────────────────────────

    [Test]
    public async Task Postgres_ConsumerAlwaysThrows_MessageDeadLetteredAfterMaxRetries()
    {
        await using var wrapper = await TestContainerWrapper.CreateContainerAsync("postgres");
        await wrapper.StartAsync();

        const int maxRetries = 3;
        var options = new SqlTransportOptions
        {
            ConnectionString  = wrapper.Container.GetConnectionString(),
            Schema            = "public",
            ContinuousPolling = true,
            MaxBatchSize      = 10,
            MaxWaitTime       = TimeSpan.FromMilliseconds(100),
            MaxRetries        = maxRetries
        };

        var consumer = new AlwaysThrowingConsumer<OrderSubmitted>();
        var logger   = Moq.Mock.Of<ILogger<PostgresMessageBus>>();

        await using var bus = new PostgresMessageBus(options, logger);
        bus.Subscribe(consumer);

        await bus.Publish(new OrderSubmitted { OrderId = Guid.NewGuid(), ProductName = "Retry test" });

        // Allow all retry cycles to run. MaxRetries=3 means 3 delivery attempts; each
        // cycle is near-instant when ContinuousPolling=true and the queue is not empty.
        await Task.Delay(4000);

        Assert.That(consumer.CallCount, Is.EqualTo(maxRetries),
            "Consumer should be called exactly MaxRetries times before dead-lettering.");

        await using var cn = wrapper.CreateConnection();
        await cn.OpenAsync();

        var row = await cn.QueryFirstAsync(
            "SELECT retrycount, failedat FROM public.workqueue WHERE channel = @ch",
            new { ch = typeof(OrderSubmitted).FullName });

        Assert.That((int)row.retrycount, Is.EqualTo(maxRetries),
            "retrycount should equal MaxRetries.");
        Assert.That((DateTime?)row.failedat, Is.Not.Null,
            "failedat should be set once the message is dead-lettered.");
    }

    [Test]
    public async Task Postgres_MessageDeadLettered_IsNotRedelivered()
    {
        await using var wrapper = await TestContainerWrapper.CreateContainerAsync("postgres");
        await wrapper.StartAsync();

        const int maxRetries = 2;
        var options = new SqlTransportOptions
        {
            ConnectionString  = wrapper.Container.GetConnectionString(),
            Schema            = "public",
            ContinuousPolling = true,
            MaxBatchSize      = 10,
            MaxWaitTime       = TimeSpan.FromMilliseconds(100),
            MaxRetries        = maxRetries
        };

        var consumer = new AlwaysThrowingConsumer<OrderSubmitted>();
        var logger   = Moq.Mock.Of<ILogger<PostgresMessageBus>>();

        await using var bus = new PostgresMessageBus(options, logger);
        bus.Subscribe(consumer);

        await bus.Publish(new OrderSubmitted { OrderId = Guid.NewGuid(), ProductName = "Dead-letter test" });

        // Wait for dead-lettering, then an extra second to confirm no further deliveries.
        await Task.Delay(4000);
        int countAfterDeadLetter = consumer.CallCount;

        await Task.Delay(1500);

        Assert.That(consumer.CallCount, Is.EqualTo(countAfterDeadLetter),
            "No further deliveries should occur after a message is dead-lettered.");
    }

    // ── SQL Server ────────────────────────────────────────────────────────────

    [Test]
    public async Task SqlServer_ConsumerAlwaysThrows_MessageDeadLetteredAfterMaxRetries()
    {
        await using var wrapper = await TestContainerWrapper.CreateContainerAsync("mssql");
        await wrapper.StartAsync();

        const int maxRetries = 3;
        var options = new SqlServerTransportOptions
        {
            ConnectionString  = wrapper.Container.GetConnectionString(),
            Schema            = "dbo",
            ContinuousPolling = true,
            MaxBatchSize      = 10,
            MaxWaitTime       = TimeSpan.FromMilliseconds(100),
            MaxRetries        = maxRetries
        };

        var consumer = new AlwaysThrowingConsumer<OrderSubmitted>();
        var logger   = Moq.Mock.Of<ILogger<SqlServerMessageBus>>();

        await using var bus = new SqlServerMessageBus(options, logger);
        bus.Subscribe(consumer);

        await bus.Publish(new OrderSubmitted { OrderId = Guid.NewGuid(), ProductName = "SQL Server retry test" });

        await Task.Delay(4000);

        Assert.That(consumer.CallCount, Is.EqualTo(maxRetries),
            "Consumer should be called exactly MaxRetries times before dead-lettering.");

        await using var cn = wrapper.CreateConnection();
        await cn.OpenAsync();

        var row = await cn.QueryFirstAsync(
            "SELECT retrycount, failedat FROM [dbo].[workqueue] WHERE channel = @ch",
            new { ch = typeof(OrderSubmitted).FullName });

        Assert.That((int)row.retrycount, Is.EqualTo(maxRetries));
        Assert.That((DateTime?)row.failedat, Is.Not.Null);
    }

    [Test]
    public async Task SqlServer_MessageDeadLettered_IsNotRedelivered()
    {
        await using var wrapper = await TestContainerWrapper.CreateContainerAsync("mssql");
        await wrapper.StartAsync();

        const int maxRetries = 2;
        var options = new SqlServerTransportOptions
        {
            ConnectionString  = wrapper.Container.GetConnectionString(),
            Schema            = "dbo",
            ContinuousPolling = true,
            MaxBatchSize      = 10,
            MaxWaitTime       = TimeSpan.FromMilliseconds(100),
            MaxRetries        = maxRetries
        };

        var consumer = new AlwaysThrowingConsumer<OrderSubmitted>();
        var logger   = Moq.Mock.Of<ILogger<SqlServerMessageBus>>();

        await using var bus = new SqlServerMessageBus(options, logger);
        bus.Subscribe(consumer);

        await bus.Publish(new OrderSubmitted { OrderId = Guid.NewGuid(), ProductName = "SQL Server dead-letter test" });

        await Task.Delay(4000);
        int countAfterDeadLetter = consumer.CallCount;

        await Task.Delay(1500);

        Assert.That(consumer.CallCount, Is.EqualTo(countAfterDeadLetter),
            "No further deliveries should occur after a message is dead-lettered.");
    }

    // ── Redis ─────────────────────────────────────────────────────────────────

    [Test]
    public async Task Redis_ConsumerAlwaysThrows_MessageMovedToDeadLetterStream()
    {
        await using var container = new RedisBuilder().Build();
        await container.StartAsync();

        using var mux = ConnectionMultiplexer.Connect(container.GetConnectionString());

        const int maxRetries = 3;
        // ReclaimIdleTime must be short so XAUTOCLAIM fires quickly in tests.
        var options = new RedisOptions
        {
            KeyPrefix       = "dltest",
            ConsumerGroup   = "test-group",
            ConsumerName    = "test-consumer",
            MaxBatchSize    = 10,
            MaxWaitTime     = TimeSpan.FromMilliseconds(100),
            MaxRetries      = maxRetries,
            ReclaimIdleTime = TimeSpan.FromMilliseconds(200)
        };

        var consumer = new AlwaysThrowingConsumer<OrderSubmitted>();
        var logger   = Moq.Mock.Of<ILogger<RedisMessageBus>>();

        await using var bus = new RedisMessageBus(mux, options, logger);
        bus.Subscribe(consumer);

        await bus.Publish(new OrderSubmitted { OrderId = Guid.NewGuid(), ProductName = "Redis retry test" });

        // Each retry requires one XAUTOCLAIM cycle (after ReclaimIdleTime=200ms).
        // MaxRetries=3 → first delivery + 2 reclaims → 3 total calls → dead-letter on 3rd.
        await Task.Delay(6000);

        var db         = mux.GetDatabase();
        var streamKey  = $"dltest:stream:{typeof(OrderSubmitted).FullName}";
        var deadKey    = $"{streamKey}:dead";

        var deadLength = await db.StreamLengthAsync(deadKey);
        Assert.That(deadLength, Is.EqualTo(1), "Dead-letter stream should contain exactly one entry.");

        // The main stream's pending list should be empty (message was ACK-ed on dead-letter).
        var pending = await db.StreamPendingAsync(streamKey, options.ConsumerGroup);
        Assert.That(pending.PendingMessageCount, Is.EqualTo(0),
            "No pending messages should remain after dead-lettering.");
    }
}

// ── Shared helpers ────────────────────────────────────────────────────────────

internal class AlwaysThrowingConsumer<T> : IConsumer<T>
{
    private int _callCount;
    public int CallCount => _callCount;

    public Task Consume(ConsumeContext<T> context)
    {
        Interlocked.Increment(ref _callCount);
        throw new InvalidOperationException("Simulated consumer failure for retry/dead-letter testing.");
    }
}
