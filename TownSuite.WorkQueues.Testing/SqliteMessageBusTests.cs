using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using TownSuite.WorkQueues.Sqlite;

namespace TownSuite.WorkQueues.Testing;

[TestFixture]
public class SqliteMessageBusTests
{
    private string _dbPath = string.Empty;
    private string _connectionString = string.Empty;
    private SqliteTransportOptions _defaultOptions = null!;

    [SetUp]
    public async Task SetUp()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"wq-test-{Guid.NewGuid()}.db");
        _connectionString = $"Data Source={_dbPath}";
        _defaultOptions = MakeOptions();
        await RunMigrationAsync(_defaultOptions);
    }

    [TearDown]
    public void TearDown()
    {
        foreach (var f in new[] { _dbPath, _dbPath + "-wal", _dbPath + "-shm" })
            if (File.Exists(f)) File.Delete(f);
    }

    // ── Basic delivery ────────────────────────────────────────────────────────

    [Test]
    public async Task PublishSubscribe_DeliversAllMessages()
    {
        var consumer = new CountingSqliteConsumer();
        await using var bus = new SqliteMessageBus(_defaultOptions, MakeLogger());
        bus.Subscribe(consumer);

        for (int i = 0; i < 10; i++)
            await bus.Publish(new OrderSubmitted { OrderId = Guid.NewGuid(), ProductName = $"Item {i}" });

        await WaitForCount(() => consumer.Count, 10);

        Assert.That(consumer.Count, Is.EqualTo(10));
    }

    [Test]
    public async Task MultipleConsumers_EachReceivesAllMessages()
    {
        var consumerA = new CountingSqliteConsumer();
        var consumerB = new CountingSqliteConsumer();

        await using var bus = new SqliteMessageBus(_defaultOptions, MakeLogger());
        bus.Subscribe(consumerA);
        bus.Subscribe(consumerB);

        for (int i = 0; i < 5; i++)
            await bus.Publish(new OrderSubmitted { OrderId = Guid.NewGuid(), ProductName = $"Multi {i}" });

        await WaitForCount(() => consumerA.Count, 5);
        await WaitForCount(() => consumerB.Count, 5);

        Assert.That(consumerA.Count, Is.EqualTo(5));
        Assert.That(consumerB.Count, Is.EqualTo(5));
    }

    // ── ConsumeContext metadata ───────────────────────────────────────────────

    [Test]
    public async Task ConsumeContext_PopulatesMessageIdAndSentTime()
    {
        var consumer = new CapturingSqliteConsumer();
        await using var bus = new SqliteMessageBus(_defaultOptions, MakeLogger());
        bus.Subscribe(consumer);

        var before = DateTimeOffset.UtcNow.AddSeconds(-1);
        await bus.Publish(new OrderSubmitted { OrderId = Guid.NewGuid(), ProductName = "Capture" });

        await WaitFor(() => consumer.Captured != null);

        Assert.That(consumer.Captured!.MessageId, Is.Not.EqualTo(Guid.Empty));
        Assert.That(consumer.Captured.SentTime, Is.GreaterThan(before));
    }

    // ── Retry and dead-letter ─────────────────────────────────────────────────

    [Test]
    public async Task FailedMessages_AreDeadLetteredAfterMaxRetries()
    {
        var opts = MakeOptions(maxRetries: 2);
        var consumer = new ThrowingSqliteConsumer();
        await using var bus = new SqliteMessageBus(opts, MakeLogger());
        bus.Subscribe(consumer);

        await bus.Publish(new OrderSubmitted { OrderId = Guid.NewGuid(), ProductName = "Die" });

        await WaitFor(async () => await CountDeadLetteredAsync() == 1, timeoutMs: 5000);

        Assert.That(await CountDeadLetteredAsync(), Is.EqualTo(1));
        // Verify no duplicate deliveries — only MaxRetries attempts were made.
        Assert.That(consumer.CallCount, Is.EqualTo(2));
    }

    [Test]
    public async Task FaultConsumer_IsCalledWhenMessageIsDeadLettered()
    {
        var opts = MakeOptions(maxRetries: 2);
        var faultConsumer = new CountingSqliteFaultConsumer();

        await using var bus = new SqliteMessageBus(opts, MakeLogger());
        bus.Subscribe(new ThrowingSqliteConsumer());
        bus.SubscribeFault(faultConsumer);

        await bus.Publish(new OrderSubmitted { OrderId = Guid.NewGuid(), ProductName = "Fault" });

        await WaitFor(() => faultConsumer.Count == 1, timeoutMs: 5000);

        Assert.That(faultConsumer.Count, Is.EqualTo(1));
    }

    [Test]
    public async Task ReplayDeadLettered_ResetsAndRedelivers()
    {
        // MaxRetries = 1 → dead-lettered after the first failure.
        var opts = MakeOptions(maxRetries: 1);
        var consumer = new ThrowOnceSqliteConsumer();

        await using var bus = new SqliteMessageBus(opts, MakeLogger());
        bus.Subscribe(consumer);

        await bus.Publish(new OrderSubmitted { OrderId = Guid.NewGuid(), ProductName = "Replay" });

        await WaitFor(async () => await CountDeadLetteredAsync() == 1, timeoutMs: 5000);

        int replayed = await bus.ReplayDeadLettered<OrderSubmitted>();
        Assert.That(replayed, Is.EqualTo(1));

        await WaitFor(() => consumer.SuccessCount == 1, timeoutMs: 5000);

        Assert.That(consumer.SuccessCount, Is.EqualTo(1));
        Assert.That(await CountDeadLetteredAsync(), Is.EqualTo(0));
    }

    // ── Scheduled delivery ────────────────────────────────────────────────────

    [Test]
    public async Task ScheduledDelivery_WithholdsUntilDeliverAfter()
    {
        var consumer = new CountingSqliteConsumer();
        await using var bus = new SqliteMessageBus(_defaultOptions, MakeLogger());
        bus.Subscribe(consumer);

        var deliverAfter = DateTimeOffset.UtcNow.AddSeconds(2);
        await bus.Publish(
            new OrderSubmitted { OrderId = Guid.NewGuid(), ProductName = "Scheduled" },
            deliverAfter);

        await Task.Delay(500);
        Assert.That(consumer.Count, Is.EqualTo(0), "Message should not be delivered before deliverAfter");

        await WaitForCount(() => consumer.Count, 1, timeoutMs: 5000);
        Assert.That(consumer.Count, Is.EqualTo(1));
    }

    // ── Lock expiry (SQLite-specific) ─────────────────────────────────────────

    [Test]
    public async Task ExpiredLock_MessageIsReclaimedByNextPoll()
    {
        // Insert a message that is already claimed with an expired lockeduntil,
        // simulating a process that crashed after claiming but before completing.
        await InsertPreclaimedMessageAsync(
            channel: typeof(OrderSubmitted).FullName!,
            payload: """{"OrderId":"00000000-0000-0000-0000-000000000001","ProductName":"Reclaim"}""",
            expiredLockedUntil: "2000-01-01T00:00:00.000Z");

        var consumer = new CountingSqliteConsumer();
        await using var bus = new SqliteMessageBus(_defaultOptions, MakeLogger());
        bus.Subscribe(consumer);

        await WaitForCount(() => consumer.Count, 1, timeoutMs: 5000);

        Assert.That(consumer.Count, Is.EqualTo(1));
    }

    // ── IsPolling ─────────────────────────────────────────────────────────────

    [Test]
    public async Task IsPolling_TrueWhileRunningFalseAfterDispose()
    {
        var bus = new SqliteMessageBus(_defaultOptions, MakeLogger());
        Assert.That(bus.IsPolling, Is.True);

        await bus.DisposeAsync();
        Assert.That(bus.IsPolling, Is.False);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private SqliteTransportOptions MakeOptions(int maxRetries = 3) => new()
    {
        ConnectionString  = _connectionString,
        ContinuousPolling = true,
        MaxBatchSize      = 10,
        MaxWaitTime       = TimeSpan.FromMilliseconds(100),
        MaxRetries        = maxRetries
    };

    private static ILogger<SqliteMessageBus> MakeLogger() =>
        Moq.Mock.Of<ILogger<SqliteMessageBus>>();

    private static async Task RunMigrationAsync(SqliteTransportOptions options)
    {
        var sp = new ServiceCollection()
            .AddSingleton(options)
            .AddLogging()
            .BuildServiceProvider();
        var logger = sp.GetRequiredService<ILogger<SqliteMigrationHostedService>>();
        var migration = new SqliteMigrationHostedService(sp, logger);
        await migration.StartAsync(CancellationToken.None);
    }

    private async Task<int> CountDeadLetteredAsync()
    {
        await using var conn = new SqliteConnection(_connectionString);
        await conn.OpenAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM workqueue WHERE failedat IS NOT NULL";
        return Convert.ToInt32(await cmd.ExecuteScalarAsync());
    }

    private async Task InsertPreclaimedMessageAsync(string channel, string payload, string expiredLockedUntil)
    {
        await using var conn = new SqliteConnection(_connectionString);
        await conn.OpenAsync();
        var cmd = conn.CreateCommand();
        cmd.CommandText = """
            INSERT INTO workqueue(channel, payload, messageid, lockeduntil, locktoken)
            VALUES(@channel, @payload, @messageid, @lockeduntil, @locktoken)
            """;
        cmd.Parameters.AddWithValue("@channel",     channel);
        cmd.Parameters.AddWithValue("@payload",     payload);
        cmd.Parameters.AddWithValue("@messageid",   Guid.NewGuid().ToString());
        cmd.Parameters.AddWithValue("@lockeduntil", expiredLockedUntil);
        cmd.Parameters.AddWithValue("@locktoken",   "dead-process-token");
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task WaitForCount(Func<int> getCount, int expected, int timeoutMs = 4000)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        while (getCount() < expected && DateTime.UtcNow < deadline)
            await Task.Delay(50);
    }

    private static async Task WaitFor(Func<bool> predicate, int timeoutMs = 4000)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        while (!predicate() && DateTime.UtcNow < deadline)
            await Task.Delay(50);
    }

    private static async Task WaitFor(Func<Task<bool>> predicate, int timeoutMs = 4000)
    {
        var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
        while (!await predicate() && DateTime.UtcNow < deadline)
            await Task.Delay(50);
    }
}

// ── Test consumers ────────────────────────────────────────────────────────────

internal class CountingSqliteConsumer : IConsumer<OrderSubmitted>
{
    private int _count;
    public int Count => _count;

    public Task Consume(ConsumeContext<OrderSubmitted> context)
    {
        Interlocked.Increment(ref _count);
        return Task.CompletedTask;
    }
}

internal class ThrowingSqliteConsumer : IConsumer<OrderSubmitted>
{
    private int _callCount;
    public int CallCount => _callCount;

    public Task Consume(ConsumeContext<OrderSubmitted> context)
    {
        Interlocked.Increment(ref _callCount);
        throw new InvalidOperationException("Simulated consumer failure");
    }
}

internal class ThrowOnceSqliteConsumer : IConsumer<OrderSubmitted>
{
    private int _calls;
    public int SuccessCount { get; private set; }

    public Task Consume(ConsumeContext<OrderSubmitted> context)
    {
        if (Interlocked.Increment(ref _calls) == 1)
            throw new InvalidOperationException("First attempt fails");
        SuccessCount++;
        return Task.CompletedTask;
    }
}

internal class CountingSqliteFaultConsumer : IConsumer<Fault<OrderSubmitted>>
{
    private int _count;
    public int Count => _count;

    public Task Consume(ConsumeContext<Fault<OrderSubmitted>> context)
    {
        Interlocked.Increment(ref _count);
        return Task.CompletedTask;
    }
}

internal class CapturingSqliteConsumer : IConsumer<OrderSubmitted>
{
    public ConsumeContext<OrderSubmitted>? Captured { get; private set; }

    public Task Consume(ConsumeContext<OrderSubmitted> context)
    {
        Captured = context;
        return Task.CompletedTask;
    }
}
