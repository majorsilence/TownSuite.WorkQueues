using System.Data.Common;
using System.Text.Json;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Npgsql;

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
            var result = await workQueue.Dequeue<JsonElement>("ASecondUniqueChannelName", cn, null!);
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
        var result = await workQueue.Dequeue<JsonElement>("AUniqueChannelName", cn, txn);
        txn.Commit();
        Assert.That(result.GetProperty("Hello").GetString(), Is.EqualTo("world"));

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
        var result1 = await workQueue.Dequeue<JsonElement>("NonDestructiveName", cn, txn1);
        var result2 = await workQueue.Dequeue<JsonElement>("NonDestructiveName", cn, txn1);
        txn1.Commit();

        Assert.That(result1.GetProperty("Hello").GetString(), Is.EqualTo("world"));
        Assert.That(result2.GetProperty("Hello").GetString(), Is.EqualTo("The Second"));

        // Column names are lowercase in both PostgreSQL and SQL Server.
        var found = cn.QueryFirstOrDefault<dynamic>("select * from workqueue where channel = 'NonDestructiveName'");
        Assert.That(found, Is.Not.Null);
        Assert.That((string)found.channel, Is.EqualTo("NonDestructiveName"));
        Assert.That(((string)found.payload).Contains("Hello"), Is.True);
        Assert.That(found.timeprocessedutc, Is.Not.Null);
    }
}
