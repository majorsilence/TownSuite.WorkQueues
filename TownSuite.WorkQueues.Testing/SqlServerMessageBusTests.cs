using Microsoft.Extensions.Logging;
using NUnit.Framework;
using TownSuite.WorkQueues.SqlServer;

namespace TownSuite.WorkQueues.Testing;

[TestFixture]
public class SqlServerMessageBusTests
{
    [Test]
    public async Task SqlServerMessageBusTest()
    {
        await using var wrapper = await TestContainerWrapper.CreateContainerAsync("mssql");
        await wrapper.StartAsync();

        var logger = Moq.Mock.Of<ILogger<SqlServerMessageBus>>();
        var options = new SqlServerTransportOptions
        {
            ConnectionString = wrapper.Container.GetConnectionString(),
            Schema           = "dbo",
            ContinuousPolling = true,
            MaxBatchSize     = 10,
            MaxWaitTime      = TimeSpan.FromSeconds(1)
        };

        var consumer = new CountingSqlServerConsumer();

        await using var bus = new SqlServerMessageBus(options, logger);
        bus.Subscribe(consumer);

        foreach (int i in Enumerable.Range(1, 10))
        {
            await bus.Publish(new OrderSubmitted
            {
                OrderId     = Guid.NewGuid(),
                ProductName = $"Widget {i} via SQL Server"
            });
        }

        await Task.Delay(4000); // Allow the polling loop to process all messages.

        Assert.That(consumer.Count, Is.EqualTo(10));
    }

    [Test]
    public async Task SqlServerMessageBus_MultipleConsumers_EachReceivesAllMessages()
    {
        await using var wrapper = await TestContainerWrapper.CreateContainerAsync("mssql");
        await wrapper.StartAsync();

        var logger = Moq.Mock.Of<ILogger<SqlServerMessageBus>>();
        var options = new SqlServerTransportOptions
        {
            ConnectionString = wrapper.Container.GetConnectionString(),
            Schema           = "dbo",
            ContinuousPolling = true,
            MaxBatchSize     = 20,
            MaxWaitTime      = TimeSpan.FromSeconds(1)
        };

        var consumerA = new CountingSqlServerConsumer();
        var consumerB = new CountingSqlServerConsumer();

        await using var bus = new SqlServerMessageBus(options, logger);
        bus.Subscribe(consumerA);
        bus.Subscribe(consumerB);

        foreach (int i in Enumerable.Range(1, 5))
        {
            await bus.Publish(new OrderSubmitted
            {
                OrderId     = Guid.NewGuid(),
                ProductName = $"Multi {i}"
            });
        }

        await Task.Delay(4000);

        Assert.That(consumerA.Count, Is.EqualTo(5));
        Assert.That(consumerB.Count, Is.EqualTo(5));
    }
}

internal class CountingSqlServerConsumer : IConsumer<OrderSubmitted>
{
    private int _count;
    public int Count => _count;

    public Task Consume(ConsumeContext<OrderSubmitted> context)
    {
        System.Threading.Interlocked.Increment(ref _count);
        return Task.CompletedTask;
    }
}
