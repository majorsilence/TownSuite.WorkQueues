using NUnit.Framework;
using StackExchange.Redis;
using Testcontainers.Redis;
using TownSuite.WorkQueues.Redis;

namespace TownSuite.WorkQueues.Testing;

[TestFixture]
public class RedisWorkQueueTests
{
    [Test]
    public async Task EnqueueDequeue_ReturnsMessage()
    {
        await using var container = new RedisBuilder().Build();
        await container.StartAsync();

        using var mux = ConnectionMultiplexer.Connect(container.GetConnectionString());
        var queue = new RedisWorkQueue(mux, new RedisOptions { KeyPrefix = "test" });

        await queue.EnqueueAsync("orders", new RedisOrderPayload { Id = 42, Name = "Widget" });
        var result = await queue.DequeueAsync<RedisOrderPayload>("orders");

        Assert.That(result, Is.Not.Null);
        Assert.That(result!.Id, Is.EqualTo(42));
        Assert.That(result.Name, Is.EqualTo("Widget"));
    }

    [Test]
    public async Task Dequeue_EmptyChannel_ReturnsNull()
    {
        await using var container = new RedisBuilder().Build();
        await container.StartAsync();

        using var mux = ConnectionMultiplexer.Connect(container.GetConnectionString());
        var queue = new RedisWorkQueue(mux, new RedisOptions { KeyPrefix = "test" });

        var result = await queue.DequeueAsync<RedisOrderPayload>("empty-channel");

        Assert.That(result, Is.Null);
    }

    [Test]
    public async Task Enqueue_MultipleMessages_DequeuesInFifoOrder()
    {
        await using var container = new RedisBuilder().Build();
        await container.StartAsync();

        using var mux = ConnectionMultiplexer.Connect(container.GetConnectionString());
        var queue = new RedisWorkQueue(mux, new RedisOptions { KeyPrefix = "test" });

        for (int i = 1; i <= 5; i++)
            await queue.EnqueueAsync("seq", new RedisOrderPayload { Id = i, Name = $"Item {i}" });

        for (int i = 1; i <= 5; i++)
        {
            var item = await queue.DequeueAsync<RedisOrderPayload>("seq");
            Assert.That(item, Is.Not.Null);
            Assert.That(item!.Id, Is.EqualTo(i));
        }

        Assert.That(await queue.DequeueAsync<RedisOrderPayload>("seq"), Is.Null);
    }
}

public class RedisOrderPayload
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
}
