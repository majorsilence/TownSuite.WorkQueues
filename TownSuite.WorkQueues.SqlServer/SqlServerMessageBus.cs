using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Text.Json;

namespace TownSuite.WorkQueues.SqlServer;

/// <summary>
/// SQL Server-backed message bus with at-least-once delivery, automatic retry,
/// and dead-lettering. Uses UPDLOCK + ROWLOCK + READPAST table hints so that
/// multiple concurrent consumer instances safely claim disjoint sets of messages
/// without blocking each other.
/// </summary>
public class SqlServerMessageBus : IMessageBus, IDisposable
{
    private readonly CancellationTokenSource _cts = new();
    private readonly ConcurrentDictionary<Type, ConcurrentBag<Func<object, Task>>> _handlers = new();
    private readonly Task _pollingTask;
    private readonly ILogger _logger;
    private readonly SqlServerTransportOptions _options;

    public SqlServerMessageBus(SqlServerTransportOptions options, ILogger logger)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _logger  = logger  ?? throw new ArgumentNullException(nameof(logger));
        _pollingTask = Task.Run(ProcessMessagesAsync);
    }

    /// <inheritdoc />
    public void Subscribe<T>(IConsumer<T> consumer)
    {
        var bag = _handlers.GetOrAdd(typeof(T), _ => new ConcurrentBag<Func<object, Task>>());
        bag.Add(async obj =>
        {
            if (obj is T message)
                await consumer.Consume(new SimpleConsumeContext<T>(message));
        });
    }

    /// <inheritdoc />
    public async Task Publish<T>(T message)
    {
        var channel = typeof(T).FullName
            ?? throw new InvalidOperationException($"Cannot determine channel name for {typeof(T)}");

        if (channel.Length > 500)
            throw new ArgumentException($"Message type name exceeds 500 characters: {channel}");

        var payload = JsonSerializer.Serialize(message);

        await using var conn = new SqlConnection(_options.ConnectionString);
        await conn.OpenAsync();

        var sql = $"INSERT INTO [{_options.Schema}].[workqueue] ([channel], [payload]) VALUES (@channel, @payload)";
        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@channel", channel);
        cmd.Parameters.AddWithValue("@payload", payload);
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task ProcessMessagesAsync()
    {
        while (!_cts.Token.IsCancellationRequested)
        {
            try
            {
                int processed = await ClaimMessagesAsync(_options.MaxBatchSize);
                if (processed == 0)
                    await Task.Delay(_options.MaxWaitTime, _cts.Token);
            }
            catch (TaskCanceledException) { }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in SqlServerMessageBus polling loop");
                try { await Task.Delay(1000, _cts.Token); } catch (TaskCanceledException) { }
            }
        }
    }

    private async Task<int> ClaimMessagesAsync(int maxMessages)
    {
        if (_handlers.Count == 0) return 0;

        var channelNames = _handlers.Keys
            .Select(t => t.FullName)
            .OfType<string>()
            .ToArray();

        if (channelNames.Length == 0) return 0;

        // SQL Server has no array parameter type. Build a safe IN clause using
        // auto-named parameters — channel names come from typeof(T).FullName so
        // are developer-controlled, not user input.
        var paramNames = Enumerable.Range(0, channelNames.Length)
            .Select(i => $"@ch{i}")
            .ToArray();
        var inClause = string.Join(", ", paramNames);

        // UPDLOCK + ROWLOCK + READPAST: this connection claims the rows exclusively;
        // other connections with the same hints skip these rows rather than blocking.
        var selectSql = $"""
            SELECT TOP (@maxMessages) [id], [channel], [payload]
            FROM [{_options.Schema}].[workqueue] WITH (UPDLOCK, ROWLOCK, READPAST)
            WHERE [timeprocessedutc] IS NULL
              AND [failedat] IS NULL
              AND [channel] IN ({inClause})
            ORDER BY [timecreatedutc]
            """;

        var messages = new List<MessageDto>();

        await using var conn = new SqlConnection(_options.ConnectionString);
        await conn.OpenAsync();
        await using var tran = conn.BeginTransaction();

        await using (var cmd = new SqlCommand(selectSql, conn, tran))
        {
            cmd.Parameters.AddWithValue("@maxMessages", maxMessages);
            for (int i = 0; i < channelNames.Length; i++)
                cmd.Parameters.AddWithValue(paramNames[i], channelNames[i]);

            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                messages.Add(new MessageDto
                {
                    Id      = reader.GetInt32(0),
                    Channel = reader.GetString(1),
                    Payload = reader.GetString(2)
                });
            }
        }

        foreach (var msg in messages)
        {
            bool success = false;
            try
            {
                await DispatchMessageAsync(msg);
                success = true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Handler failed for message {Id} on channel {Channel}",
                    msg.Id, msg.Channel);
            }

            if (success)
            {
                await using var upd = new SqlCommand(
                    $"UPDATE [{_options.Schema}].[workqueue] SET [timeprocessedutc] = GETUTCDATE() WHERE [id] = @id",
                    conn, tran);
                upd.Parameters.AddWithValue("@id", msg.Id);
                await upd.ExecuteNonQueryAsync();
            }
            else
            {
                await using var retry = new SqlCommand($"""
                    UPDATE [{_options.Schema}].[workqueue]
                    SET [retrycount] = [retrycount] + 1,
                        [failedat]   = CASE WHEN [retrycount] + 1 >= @maxRetries
                                            THEN GETUTCDATE()
                                            ELSE NULL
                                       END
                    WHERE [id] = @id
                    """, conn, tran);
                retry.Parameters.AddWithValue("@id", msg.Id);
                retry.Parameters.AddWithValue("@maxRetries", _options.MaxRetries);
                await retry.ExecuteNonQueryAsync();
            }
        }

        await tran.CommitAsync();
        return messages.Count;
    }

    private async Task DispatchMessageAsync(MessageDto msg)
    {
        var type = _handlers.Keys.FirstOrDefault(t => t.FullName == msg.Channel);
        if (type == null)
        {
            _logger.LogWarning(
                "No handler registered for channel {Channel} — message {Id} will be skipped",
                msg.Channel, msg.Id);
            return;
        }

        if (!_handlers.TryGetValue(type, out var handlers)) return;

        var message = LegacyJsonDeserializer.Deserialize(msg.Payload, type);
        await Task.WhenAll(handlers.Select(h => h(message!)));
    }

    public void Dispose()
    {
        _cts.Cancel();
        try { _pollingTask?.Wait(TimeSpan.FromSeconds(10)); }
        catch (AggregateException) { }
        _cts.Dispose();
    }
}
