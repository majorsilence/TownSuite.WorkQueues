using System.Data.Common;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Npgsql;
using Testcontainers.MsSql;
using Testcontainers.PostgreSql;

namespace TownSuite.WorkQueues.Testing;

[TestFixture]
public class DatabasedBackedTest
{
    [TestCase("mssql")]
    [TestCase("postgres")]
    public async Task DequeueMustHaveTransactionTest1(string backend)
    {
        await using var wrapper = await TestContainerWrapper.CreateContainerAsync(backend);
        await wrapper.StartAsync();

        await using var cn = wrapper.CreateConnection();
        await cn.OpenAsync();

        var workQueue = new DbBackedWorkQueue();

        Assert.ThrowsAsync<WorkQueuesException>(async () =>
        {
            var result = await workQueue.Dequeue<dynamic>("ASecondUniqueChannelName", cn, null);
        });
    }

    [TestCase("mssql")]
    [TestCase("postgres")]
    public async Task EnqueueAndDequeue_Test(string backend)
    {
        await using var wrapper = await TestContainerWrapper.CreateContainerAsync(backend);
        await wrapper.StartAsync();

        await using var cn = wrapper.CreateConnection();
        await cn.OpenAsync();

        var workQueue = new DbBackedWorkQueue();

        await workQueue.Enqueue("AUniqueChannelName",
            new { Hello = "world" }, cn, null);

        await using var txn = cn.BeginTransaction();
        var result = await workQueue.Dequeue<dynamic>("AUniqueChannelName", cn, txn);
        txn.Commit();
        Assert.That(string.Equals(result.ToString(), "{ Hello = world }"));

        var found = cn.QueryFirstOrDefault("select * from workqueue where channel = 'AUniqueChannelName'");
        Assert.That(found == null);
    }

    [TestCase("mssql")]
    [TestCase("postgres")]
    public async Task EnqueueAndDequeue_NonDestructive_Test(string backend)
    {
        await using var wrapper = await TestContainerWrapper.CreateContainerAsync(backend);
        await wrapper.StartAsync();

        await using var cn = wrapper.CreateConnection();
        await cn.OpenAsync();

        var workQueue = new DbBackedWorkQueue_NonDestructive();

        await workQueue.Enqueue("NonDestructiveName",
            new { Hello = "world" }, cn, null);
        await workQueue.Enqueue("NonDestructiveName",
            new { Hello = "The Second" }, cn, null);

        await using var txn1 = cn.BeginTransaction();
        var result1 = await workQueue.Dequeue<dynamic>("NonDestructiveName", cn, txn1);
        var result2 = await workQueue.Dequeue<dynamic>("NonDestructiveName", cn, txn1);
        txn1.Commit();
        Assert.That(string.Equals(result2.ToString(), "{ Hello = The Second }"));

        var found = cn.QueryFirstOrDefault<dynamic>("select * from workqueue where channel = 'NonDestructiveName'");
        Assert.That(found != null);
        Assert.That(found.Channel == "NonDestructiveName");
        Assert.That(found.Payload.Contains("\"Hello\": \"world\""));
        Assert.That(found.timeprocessedutc != null);
    }

 
}
