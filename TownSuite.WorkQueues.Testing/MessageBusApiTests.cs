using Microsoft.Extensions.Logging;
using NUnit.Framework;
using StackExchange.Redis;
using Testcontainers.Redis;
using TownSuite.WorkQueues.Postgres;
using TownSuite.WorkQueues.Redis;
using TownSuite.WorkQueues.SqlServer;

namespace TownSuite.WorkQueues.Testing;

/// <summary>
/// Integration tests covering the newer IMessageBus surface:
/// IsPolling, Subscribe idempotency, ConsumeContext.CancellationToken,
/// Publish cancellation, and backend-parity tests.
/// </summary>
[TestFixture]
public class MessageBusApiTests
{
    // ── IsPolling ─────────────────────────────────────────────────────────────

    [Test]
    public async Task IsPolling_TrueWhileRunning_FalseAfterDispose()
    {
        await using var wrapper = await TestContainerWrapper.CreateContainerAsync("postgres");
        await wrapper.StartAsync();

        var options = new SqlTransportOptions
        {
            ConnectionString = wrapper.Container.GetConnectionString(),
            Schema           = "public",
            MaxWaitTime      = TimeSpan.FromMilliseconds(200)
        };

        var bus = new PostgresMessageBus(options, Moq.Mock.Of<ILogger<PostgresMessageBus>>());

        // Give the polling task a moment to start.
        await Task.Delay(200);
        Assert.That(bus.IsPolling, Is.True, "Bus should report IsPolling=true while the loop is running.");

        await bus.DisposeAsync();

        Assert.That(bus.IsPolling, Is.False, "Bus should report IsPolling=false after DisposeAsync.");
    }

    // ── Subscribe idempotency ─────────────────────────────────────────────────

    [Test]
    public async Task Subscribe_SameConsumerTwice_MessageDeliveredOnce()
    {
        // Uses Redis because it spins up quickly and we want to isolate this one behaviour.
        await using var container = new RedisBuilder().Build();
        await container.StartAsync();

        using var mux = ConnectionMultiplexer.Connect(container.GetConnectionString());

        var options = new RedisOptions
        {
            KeyPrefix     = "dedup-test",
            ConsumerGroup = "g",
            MaxBatchSize  = 10,
            MaxWaitTime   = TimeSpan.FromMilliseconds(100)
        };

        var consumer = new CountingApiConsumer();

        await using var bus = new RedisMessageBus(mux, options, Moq.Mock.Of<ILogger<RedisMessageBus>>());

        // Subscribing the same instance twice should be a no-op, not a double-registration.
        bus.Subscribe(consumer);
        bus.Subscribe(consumer);

        await bus.Publish(new OrderSubmitted { OrderId = Guid.NewGuid(), ProductName = "dedup" });

        await Task.Delay(2000);

        Assert.That(consumer.Count, Is.EqualTo(1),
            "Message should be delivered exactly once even when the same consumer is subscribed twice.");
    }

    // ── ConsumeContext.CancellationToken ──────────────────────────────────────

    [Test]
    public async Task Consume_ReceivesBusShutdownCancellationToken()
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

        var consumer = new TokenCapturingConsumer();

        await using var bus = new PostgresMessageBus(options, Moq.Mock.Of<ILogger<PostgresMessageBus>>());
        bus.Subscribe(consumer);

        await bus.Publish(new OrderSubmitted { OrderId = Guid.NewGuid(), ProductName = "token-test" });

        await Task.Delay(2000);

        Assert.That(consumer.CapturedToken, Is.Not.EqualTo(CancellationToken.None),
            "Bus should pass a real cancellation token, not CancellationToken.None.");
        Assert.That(consumer.CapturedToken.CanBeCanceled, Is.True,
            "Token should be cancellable (sourced from the bus's internal CancellationTokenSource).");
        Assert.That(consumer.CapturedToken.IsCancellationRequested, Is.False,
            "Token should not be cancelled while the bus is still running.");
    }

    // ── Postgres — multiple consumers ─────────────────────────────────────────

    [Test]
    public async Task Postgres_MultipleConsumers_EachReceivesAllMessages()
    {
        await using var wrapper = await TestContainerWrapper.CreateContainerAsync("postgres");
        await wrapper.StartAsync();

        var options = new SqlTransportOptions
        {
            ConnectionString  = wrapper.Container.GetConnectionString(),
            Schema            = "public",
            ContinuousPolling = true,
            MaxBatchSize      = 20,
            MaxWaitTime       = TimeSpan.FromMilliseconds(100)
        };

        var consumerA = new CountingApiConsumer();
        var consumerB = new CountingApiConsumer();

        await using var bus = new PostgresMessageBus(options, Moq.Mock.Of<ILogger<PostgresMessageBus>>());
        bus.Subscribe(consumerA);
        bus.Subscribe(consumerB);

        for (int i = 0; i < 5; i++)
            await bus.Publish(new OrderSubmitted { OrderId = Guid.NewGuid(), ProductName = $"Fan-out {i}" });

        await Task.Delay(4000);

        Assert.That(consumerA.Count, Is.EqualTo(5), "Consumer A should receive all 5 messages.");
        Assert.That(consumerB.Count, Is.EqualTo(5), "Consumer B should receive all 5 messages.");
    }

    // ── Publish with cancelled token ──────────────────────────────────────────

    [Test]
    public async Task Publish_WithAlreadyCancelledToken_ThrowsOperationCanceledException()
    {
        await using var container = new RedisBuilder().Build();
        await container.StartAsync();

        using var mux = ConnectionMultiplexer.Connect(container.GetConnectionString());

        await using var bus = new RedisMessageBus(
            mux,
            new RedisOptions { KeyPrefix = "cancel-test" },
            Moq.Mock.Of<ILogger<RedisMessageBus>>());

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.ThrowsAsync<OperationCanceledException>(
            async () => await bus.Publish(
                new OrderSubmitted { OrderId = Guid.NewGuid() },
                cts.Token),
            "Publish should throw immediately when passed an already-cancelled token.");
    }

    // ── SqlServer — IsPolling parity ──────────────────────────────────────────

    [Test]
    public async Task SqlServer_IsPolling_TrueWhileRunning_FalseAfterDispose()
    {
        await using var wrapper = await TestContainerWrapper.CreateContainerAsync("mssql");
        await wrapper.StartAsync();

        var options = new SqlServerTransportOptions
        {
            ConnectionString = wrapper.Container.GetConnectionString(),
            Schema           = "dbo",
            MaxWaitTime      = TimeSpan.FromMilliseconds(200)
        };

        var bus = new SqlServerMessageBus(options, Moq.Mock.Of<ILogger<SqlServerMessageBus>>());

        await Task.Delay(200);
        Assert.That(bus.IsPolling, Is.True);

        await bus.DisposeAsync();

        Assert.That(bus.IsPolling, Is.False);
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

internal class CountingApiConsumer : IConsumer<OrderSubmitted>
{
    private int _count;
    public int Count => _count;

    public Task Consume(ConsumeContext<OrderSubmitted> context)
    {
        Interlocked.Increment(ref _count);
        return Task.CompletedTask;
    }
}

internal class TokenCapturingConsumer : IConsumer<OrderSubmitted>
{
    public CancellationToken CapturedToken { get; private set; } = CancellationToken.None;

    public Task Consume(ConsumeContext<OrderSubmitted> context)
    {
        CapturedToken = context.CancellationToken;
        return Task.CompletedTask;
    }
}
