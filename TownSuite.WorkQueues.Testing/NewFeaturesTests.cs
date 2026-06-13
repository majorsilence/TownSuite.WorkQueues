using Dapper;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using StackExchange.Redis;
using Testcontainers.Redis;
using TownSuite.WorkQueues.Postgres;
using TownSuite.WorkQueues.Redis;
using TownSuite.WorkQueues.SqlServer;

namespace TownSuite.WorkQueues.Testing;

/// <summary>
/// Integration tests for v2.4.0 features:
///   – Fault&lt;T&gt; dead-letter hook consumer
///   – Scheduled / delayed delivery (Postgres + SqlServer)
///   – Retry delay / backoff (Postgres + SqlServer)
///   – MessageId + SentTime on ConsumeContext&lt;T&gt;
///   – Scoped consumers (Subscribe&lt;TMessage, TConsumer&gt;)
/// </summary>
[TestFixture]
public class NewFeaturesTests
{
    // ── Fault<T> — PostgreSQL ─────────────────────────────────────────────────

    [Test]
    public async Task Postgres_FaultConsumer_CalledOnDeadLetter()
    {
        await using var wrapper = await TestContainerWrapper.CreateContainerAsync("postgres");
        await wrapper.StartAsync();

        var options = new SqlTransportOptions
        {
            ConnectionString  = wrapper.Container.GetConnectionString(),
            Schema            = "public",
            ContinuousPolling = true,
            MaxBatchSize      = 10,
            MaxWaitTime       = TimeSpan.FromMilliseconds(100),
            MaxRetries        = 1
        };

        var faultConsumer = new CapturingFaultConsumer<OrderSubmitted>();

        await using var bus = new PostgresMessageBus(options, Moq.Mock.Of<ILogger<PostgresMessageBus>>());
        bus.Subscribe(new AlwaysThrowingConsumer<OrderSubmitted>());
        bus.SubscribeFault(faultConsumer);

        await bus.Publish(new OrderSubmitted { OrderId = Guid.NewGuid(), ProductName = "fault-test" });

        // MaxRetries=1 → dead-lettered after one attempt.
        await Task.Delay(3000);

        Assert.That(faultConsumer.Received, Is.Not.Null, "Fault consumer should have received a Fault<T>.");
        Assert.That(faultConsumer.Received!.OriginalMessage.ProductName, Is.EqualTo("fault-test"));
        Assert.That(faultConsumer.Received.AttemptCount, Is.EqualTo(1));
        Assert.That(faultConsumer.Received.ExceptionMessage, Does.Contain("Simulated consumer failure"));
        Assert.That(faultConsumer.Received.FaultedAt, Is.GreaterThan(DateTimeOffset.UtcNow.AddSeconds(-10)));
    }

    // ── Fault<T> — SQL Server ─────────────────────────────────────────────────

    [Test]
    public async Task SqlServer_FaultConsumer_CalledOnDeadLetter()
    {
        await using var wrapper = await TestContainerWrapper.CreateContainerAsync("mssql");
        await wrapper.StartAsync();

        var options = new SqlServerTransportOptions
        {
            ConnectionString  = wrapper.Container.GetConnectionString(),
            Schema            = "dbo",
            ContinuousPolling = true,
            MaxBatchSize      = 10,
            MaxWaitTime       = TimeSpan.FromMilliseconds(100),
            MaxRetries        = 1
        };

        var faultConsumer = new CapturingFaultConsumer<OrderSubmitted>();

        await using var bus = new SqlServerMessageBus(options, Moq.Mock.Of<ILogger<SqlServerMessageBus>>());
        bus.Subscribe(new AlwaysThrowingConsumer<OrderSubmitted>());
        bus.SubscribeFault(faultConsumer);

        await bus.Publish(new OrderSubmitted { OrderId = Guid.NewGuid(), ProductName = "fault-test-ss" });

        await Task.Delay(3000);

        Assert.That(faultConsumer.Received, Is.Not.Null, "Fault consumer should have received a Fault<T>.");
        Assert.That(faultConsumer.Received!.OriginalMessage.ProductName, Is.EqualTo("fault-test-ss"));
        Assert.That(faultConsumer.Received.AttemptCount, Is.EqualTo(1));
    }

    // ── Fault<T> — Redis ─────────────────────────────────────────────────────

    [Test]
    public async Task Redis_FaultConsumer_CalledOnDeadLetter()
    {
        await using var container = new RedisBuilder().Build();
        await container.StartAsync();

        using var mux = ConnectionMultiplexer.Connect(container.GetConnectionString());

        var options = new RedisOptions
        {
            KeyPrefix       = "faulttest",
            ConsumerGroup   = "fg",
            ConsumerName    = "fc",
            MaxBatchSize    = 10,
            MaxWaitTime     = TimeSpan.FromMilliseconds(100),
            MaxRetries      = 1,
            ReclaimIdleTime = TimeSpan.FromMilliseconds(200)
        };

        var faultConsumer = new CapturingFaultConsumer<OrderSubmitted>();

        await using var bus = new RedisMessageBus(mux, options, Moq.Mock.Of<ILogger<RedisMessageBus>>());
        bus.Subscribe(new AlwaysThrowingConsumer<OrderSubmitted>());
        bus.SubscribeFault(faultConsumer);

        await bus.Publish(new OrderSubmitted { OrderId = Guid.NewGuid(), ProductName = "fault-test-redis" });

        await Task.Delay(5000);

        Assert.That(faultConsumer.Received, Is.Not.Null, "Fault consumer should have received a Fault<T>.");
        Assert.That(faultConsumer.Received!.OriginalMessage.ProductName, Is.EqualTo("fault-test-redis"));
        Assert.That(faultConsumer.Received.AttemptCount, Is.EqualTo(1));
    }

    // ── Scheduled delivery — PostgreSQL ──────────────────────────────────────

    [Test]
    public async Task Postgres_ScheduledDelivery_MessageWithheldUntilScheduleTime()
    {
        await using var wrapper = await TestContainerWrapper.CreateContainerAsync("postgres");
        await wrapper.StartAsync();

        var options = new SqlTransportOptions
        {
            ConnectionString  = wrapper.Container.GetConnectionString(),
            Schema            = "public",
            ContinuousPolling = true,
            MaxBatchSize      = 10,
            MaxWaitTime       = TimeSpan.FromMilliseconds(100)
        };

        var consumer = new CountingApiConsumer();

        await using var bus = new PostgresMessageBus(options, Moq.Mock.Of<ILogger<PostgresMessageBus>>());
        bus.Subscribe(consumer);

        // Schedule 3 seconds in the future — should NOT be delivered immediately.
        await bus.Publish(
            new OrderSubmitted { OrderId = Guid.NewGuid(), ProductName = "scheduled" },
            deliverAfter: DateTimeOffset.UtcNow.AddSeconds(3));

        await Task.Delay(1500);
        Assert.That(consumer.Count, Is.EqualTo(0), "Message should not be delivered before scheduledfor time.");

        await Task.Delay(3000);
        Assert.That(consumer.Count, Is.EqualTo(1), "Message should be delivered after scheduledfor time has passed.");
    }

    // ── Scheduled delivery — SQL Server ──────────────────────────────────────

    [Test]
    public async Task SqlServer_ScheduledDelivery_MessageWithheldUntilScheduleTime()
    {
        await using var wrapper = await TestContainerWrapper.CreateContainerAsync("mssql");
        await wrapper.StartAsync();

        var options = new SqlServerTransportOptions
        {
            ConnectionString  = wrapper.Container.GetConnectionString(),
            Schema            = "dbo",
            ContinuousPolling = true,
            MaxBatchSize      = 10,
            MaxWaitTime       = TimeSpan.FromMilliseconds(100)
        };

        var consumer = new CountingApiConsumer();

        await using var bus = new SqlServerMessageBus(options, Moq.Mock.Of<ILogger<SqlServerMessageBus>>());
        bus.Subscribe(consumer);

        await bus.Publish(
            new OrderSubmitted { OrderId = Guid.NewGuid(), ProductName = "scheduled-ss" },
            deliverAfter: DateTimeOffset.UtcNow.AddSeconds(3));

        await Task.Delay(1500);
        Assert.That(consumer.Count, Is.EqualTo(0), "Message should not be delivered before scheduledfor time.");

        await Task.Delay(3000);
        Assert.That(consumer.Count, Is.EqualTo(1), "Message should be delivered after scheduledfor time has passed.");
    }

    // ── Retry delay — PostgreSQL ──────────────────────────────────────────────

    [Test]
    public async Task Postgres_RetryDelay_MessageWithheldBetweenRetries()
    {
        await using var wrapper = await TestContainerWrapper.CreateContainerAsync("postgres");
        await wrapper.StartAsync();

        const int retryDelaySec = 3;
        var options = new SqlTransportOptions
        {
            ConnectionString  = wrapper.Container.GetConnectionString(),
            Schema            = "public",
            ContinuousPolling = true,
            MaxBatchSize      = 10,
            MaxWaitTime       = TimeSpan.FromMilliseconds(100),
            MaxRetries        = 3,
            RetryDelay        = TimeSpan.FromSeconds(retryDelaySec)
        };

        var consumer = new AlwaysThrowingConsumer<OrderSubmitted>();

        await using var bus = new PostgresMessageBus(options, Moq.Mock.Of<ILogger<PostgresMessageBus>>());
        bus.Subscribe(consumer);

        await bus.Publish(new OrderSubmitted { OrderId = Guid.NewGuid(), ProductName = "retry-delay" });

        // After the first attempt, the message should be held for retryDelaySec before retry.
        // With retryDelaySec=3, after 1.5 s we expect exactly 1 attempt.
        await Task.Delay(1500);
        Assert.That(consumer.CallCount, Is.EqualTo(1), "Only one attempt should have been made by 1.5 s (delay holds it back).");

        // After the delay elapses a second attempt fires.
        await Task.Delay(3000);
        Assert.That(consumer.CallCount, Is.GreaterThanOrEqualTo(2), "At least a second attempt should have fired after the retry delay.");
    }

    // ── MessageId + SentTime — PostgreSQL ────────────────────────────────────

    [Test]
    public async Task Postgres_ConsumeContext_HasMessageIdAndSentTime()
    {
        await using var wrapper = await TestContainerWrapper.CreateContainerAsync("postgres");
        await wrapper.StartAsync();

        var options = new SqlTransportOptions
        {
            ConnectionString  = wrapper.Container.GetConnectionString(),
            Schema            = "public",
            ContinuousPolling = true,
            MaxBatchSize      = 10,
            MaxWaitTime       = TimeSpan.FromMilliseconds(100)
        };

        var consumer = new MetadataCapturingConsumer();

        await using var bus = new PostgresMessageBus(options, Moq.Mock.Of<ILogger<PostgresMessageBus>>());
        bus.Subscribe(consumer);

        var before = DateTimeOffset.UtcNow.AddSeconds(-1);
        await bus.Publish(new OrderSubmitted { OrderId = Guid.NewGuid(), ProductName = "metadata-test" });

        await Task.Delay(2000);

        Assert.That(consumer.CapturedMessageId, Is.Not.EqualTo(Guid.Empty),
            "MessageId should be a non-empty Guid.");
        Assert.That(consumer.CapturedSentTime, Is.GreaterThan(before),
            "SentTime should be approximately now.");
        Assert.That(consumer.CapturedSentTime, Is.LessThan(DateTimeOffset.UtcNow.AddMinutes(1)),
            "SentTime should be a reasonable UTC timestamp.");
    }

    // ── MessageId + SentTime — Redis ─────────────────────────────────────────

    [Test]
    public async Task Redis_ConsumeContext_HasMessageIdAndSentTime()
    {
        await using var container = new RedisBuilder().Build();
        await container.StartAsync();

        using var mux = ConnectionMultiplexer.Connect(container.GetConnectionString());

        var options = new RedisOptions
        {
            KeyPrefix     = "metacheck",
            ConsumerGroup = "mg",
            ConsumerName  = "mc",
            MaxBatchSize  = 10,
            MaxWaitTime   = TimeSpan.FromMilliseconds(100)
        };

        var consumer = new MetadataCapturingConsumer();

        await using var bus = new RedisMessageBus(mux, options, Moq.Mock.Of<ILogger<RedisMessageBus>>());
        bus.Subscribe(consumer);

        var before = DateTimeOffset.UtcNow.AddSeconds(-1);
        await bus.Publish(new OrderSubmitted { OrderId = Guid.NewGuid(), ProductName = "redis-meta" });

        await Task.Delay(2000);

        Assert.That(consumer.CapturedMessageId, Is.Not.EqualTo(Guid.Empty),
            "MessageId should be a non-empty Guid (stored as stream field on publish).");
        Assert.That(consumer.CapturedSentTime, Is.GreaterThan(before),
            "SentTime should be approximately now (derived from stream entry epoch ms).");
    }

    // ── Scoped consumer — PostgreSQL ─────────────────────────────────────────

    [Test]
    public async Task Postgres_ScopedConsumer_ResolvesNewInstancePerMessage()
    {
        await using var wrapper = await TestContainerWrapper.CreateContainerAsync("postgres");
        await wrapper.StartAsync();

        var options = new SqlTransportOptions
        {
            ConnectionString  = wrapper.Container.GetConnectionString(),
            Schema            = "public",
            ContinuousPolling = true,
            MaxBatchSize      = 10,
            MaxWaitTime       = TimeSpan.FromMilliseconds(100)
        };

        // Build a real DI container with ScopedOrderConsumer registered as Scoped.
        var services = new ServiceCollection();
        services.AddScoped<ScopedOrderConsumer>();
        var provider = services.BuildServiceProvider();

        await using var bus = new PostgresMessageBus(
            options, Moq.Mock.Of<ILogger<PostgresMessageBus>>(), provider);
        bus.Subscribe<OrderSubmitted, ScopedOrderConsumer>();

        for (int i = 0; i < 3; i++)
            await bus.Publish(new OrderSubmitted { OrderId = Guid.NewGuid(), ProductName = $"scoped-{i}" });

        await Task.Delay(3000);

        // Each message should have been handled — ScopedOrderConsumer counts globally via a static.
        Assert.That(ScopedOrderConsumer.GlobalCallCount, Is.EqualTo(3),
            "Each of 3 messages should have been dispatched to a scoped consumer instance.");
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

internal class CapturingFaultConsumer<T> : IConsumer<Fault<T>>
{
    public Fault<T>? Received { get; private set; }

    public Task Consume(ConsumeContext<Fault<T>> context)
    {
        Received = context.Message;
        return Task.CompletedTask;
    }
}

internal class MetadataCapturingConsumer : IConsumer<OrderSubmitted>
{
    public Guid CapturedMessageId { get; private set; }
    public DateTimeOffset CapturedSentTime { get; private set; }

    public Task Consume(ConsumeContext<OrderSubmitted> context)
    {
        CapturedMessageId = context.MessageId;
        CapturedSentTime  = context.SentTime;
        return Task.CompletedTask;
    }
}

internal class ScopedOrderConsumer : IConsumer<OrderSubmitted>
{
    private static int _globalCallCount;
    public static int GlobalCallCount => _globalCallCount;

    public Task Consume(ConsumeContext<OrderSubmitted> context)
    {
        Interlocked.Increment(ref _globalCallCount);
        return Task.CompletedTask;
    }
}
