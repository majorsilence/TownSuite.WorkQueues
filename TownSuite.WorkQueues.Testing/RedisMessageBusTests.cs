using Microsoft.Extensions.Logging;
using NUnit.Framework;
using StackExchange.Redis;
using Testcontainers.Redis;
using TownSuite.WorkQueues.Redis;

namespace TownSuite.WorkQueues.Testing;

[TestFixture]
public class RedisMessageBusTests
{
    [Test]
    public async Task PublishSubscribe_DeliversAllMessages()
    {
        await using var container = new RedisBuilder().Build();
        await container.StartAsync();

        using var mux = ConnectionMultiplexer.Connect(container.GetConnectionString());

        var logger = Moq.Mock.Of<ILogger<RedisMessageBus>>();
        var options = new RedisOptions
        {
            KeyPrefix     = "test",
            ConsumerGroup = "test-group",
            MaxBatchSize  = 10,
            MaxWaitTime   = TimeSpan.FromMilliseconds(200)
        };

        var consumer = new CountingRedisConsumer();

        using var bus = new RedisMessageBus(mux, options, logger);
        bus.Subscribe(consumer);

        for (int i = 0; i < 10; i++)
            await bus.Publish(new OrderSubmitted { OrderId = Guid.NewGuid(), ProductName = $"Item {i}" });

        // Allow the background polling loop time to process all messages
        await Task.Delay(3000);

        Assert.That(consumer.Count, Is.EqualTo(10));
    }

    [Test]
    public async Task Publish_MultipleConsumers_EachReceivesAllMessages()
    {
        await using var container = new RedisBuilder().Build();
        await container.StartAsync();

        using var mux = ConnectionMultiplexer.Connect(container.GetConnectionString());

        var logger = Moq.Mock.Of<ILogger<RedisMessageBus>>();
        var options = new RedisOptions
        {
            KeyPrefix     = "test2",
            ConsumerGroup = "test-group",
            MaxBatchSize  = 20,
            MaxWaitTime   = TimeSpan.FromMilliseconds(200)
        };

        var consumerA = new CountingRedisConsumer();
        var consumerB = new CountingRedisConsumer();

        using var bus = new RedisMessageBus(mux, options, logger);
        bus.Subscribe(consumerA);
        bus.Subscribe(consumerB);

        for (int i = 0; i < 5; i++)
            await bus.Publish(new OrderSubmitted { OrderId = Guid.NewGuid(), ProductName = $"Multi {i}" });

        await Task.Delay(3000);

        Assert.That(consumerA.Count, Is.EqualTo(5));
        Assert.That(consumerB.Count, Is.EqualTo(5));
    }
}

internal class CountingRedisConsumer : IConsumer<OrderSubmitted>
{
    private int _count;
    public int Count => _count;

    public Task Consume(ConsumeContext<OrderSubmitted> context)
    {
        System.Threading.Interlocked.Increment(ref _count);
        return Task.CompletedTask;
    }
}
