using Microsoft.Extensions.Logging;
using Npgsql;
using System.Collections.Concurrent;
using System.Text.Json;

namespace TownSuite.WorkQueues.Postgres;

public class PostgresMessageBus : IMessageBus
{
    private readonly CancellationTokenSource _cts = new CancellationTokenSource();
    private readonly ConcurrentDictionary<Type, ConcurrentBag<Func<object, Task>>> _handlers
        = new ConcurrentDictionary<Type, ConcurrentBag<Func<object, Task>>>();
    private readonly Task _pollingTask;
    private readonly ILogger _logger;
    private readonly SqlTransportOptions _options;

    public PostgresMessageBus(SqlTransportOptions options, ILogger logger)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        // Yield to the caller so Subscribe() calls made immediately after construction
        // are registered before the first poll cycle runs.
        _pollingTask = Task.Run(async () => { await Task.Yield(); await ProcessMessagesAsync(); });
    }

    public void Subscribe<T>(IConsumer<T> consumer)
    {
        var bag = _handlers.GetOrAdd(typeof(T), _ => new ConcurrentBag<Func<object, Task>>());
        bag.Add(async obj =>
        {
            if (obj is T message)
                await consumer.Consume(new SimpleConsumeContext<T>(message));
        });
    }

    public async Task Publish<T>(T message, CancellationToken cancellationToken = default)
    {
        var channel = typeof(T).FullName
            ?? throw new InvalidOperationException($"Cannot determine channel name for type {typeof(T)}");

        if (channel.Length > 500)
            throw new ArgumentException($"Message type name exceeds 500 characters: {channel}");

        var payload = JsonSerializer.Serialize(message);
        await using var conn = new NpgsqlConnection(_options.ConnectionString);
        await conn.OpenAsync(cancellationToken);
        var sql = $"INSERT INTO {_options.Schema}.workqueue(channel, payload) VALUES(@channel, @payload)";
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@channel", channel);
        cmd.Parameters.AddWithValue("@payload", payload);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    private async Task ProcessMessagesAsync()
    {
        while (!_cts.Token.IsCancellationRequested)
        {
            try
            {
                int processedCount = await ClaimMessagesAsync(_options.MaxBatchSize);
                if (processedCount == 0)
                    await WaitAsync();
            }
            catch (TaskCanceledException) { }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in PostgresMessageBus polling loop");
                try { await Task.Delay(1000, _cts.Token); } catch (TaskCanceledException) { }
            }
        }
    }

    private async Task WaitAsync()
    {
        if (!_options.AllowEmptyBatches)
            await Task.Delay(_options.MaxWaitTime, _cts.Token);
    }

    private async Task<int> ClaimMessagesAsync(int maxMessages)
    {
        if (_handlers.Count == 0)
            return 0;

        var channelNames = _handlers.Keys
            .Select(t => t.FullName)
            .OfType<string>()
            .ToArray();

        if (channelNames.Length == 0)
            return 0;

        var messages = new List<MessageDto>();

        await using var conn = new NpgsqlConnection(_options.ConnectionString);
        await conn.OpenAsync();
        await using var tran = await conn.BeginTransactionAsync();

        var selectSql = $"""
            SELECT id, channel, payload
            FROM {_options.Schema}.workqueue
            WHERE timeprocessedutc IS NULL
              AND failedat IS NULL
              AND channel = ANY(@channels)
            ORDER BY timecreatedutc
            FOR UPDATE SKIP LOCKED
            LIMIT @maxMessages
            """;

        await using (var cmd = new NpgsqlCommand(selectSql, conn, tran))
        {
            cmd.Parameters.AddWithValue("@channels", channelNames);
            cmd.Parameters.AddWithValue("@maxMessages", maxMessages);
            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                messages.Add(new MessageDto
                {
                    Id = reader.GetInt32(0),
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
                _logger.LogError(ex, "Handler failed for message {Id} on channel {Channel}", msg.Id, msg.Channel);
            }

            if (success)
            {
                await using var updateCmd = new NpgsqlCommand(
                    $"UPDATE {_options.Schema}.workqueue SET timeprocessedutc = CURRENT_TIMESTAMP WHERE id = @id",
                    conn, tran);
                updateCmd.Parameters.AddWithValue("id", msg.Id);
                await updateCmd.ExecuteNonQueryAsync();
            }
            else
            {
                // Increment retry count; dead-letter the message once MaxRetries is reached.
                await using var retryCmd = new NpgsqlCommand($"""
                    UPDATE {_options.Schema}.workqueue
                    SET retrycount = retrycount + 1,
                        failedat = CASE WHEN retrycount + 1 >= @maxRetries THEN CURRENT_TIMESTAMP ELSE NULL END
                    WHERE id = @id
                    """, conn, tran);
                retryCmd.Parameters.AddWithValue("id", msg.Id);
                retryCmd.Parameters.AddWithValue("maxRetries", _options.MaxRetries);
                await retryCmd.ExecuteNonQueryAsync();
            }
        }

        await tran.CommitAsync();
        return messages.Count;
    }

    private async Task DispatchMessageAsync(MessageDto msg)
    {
        // Resolve the type from registered handlers (avoids relying on assembly-scanning via Type.GetType).
        var type = _handlers.Keys.FirstOrDefault(t => t.FullName == msg.Channel);
        if (type == null)
        {
            _logger.LogWarning("No handlers registered for channel: {Channel} — message {Id} will be skipped", msg.Channel, msg.Id);
            return;
        }

        if (!_handlers.TryGetValue(type, out var handlers))
            return;

        var message = LegacyJsonDeserializer.Deserialize(msg.Payload, type);
        await Task.WhenAll(handlers.Select(h => h(message!)));
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        try { await _pollingTask.WaitAsync(TimeSpan.FromSeconds(10)); }
        catch (OperationCanceledException) { }
        catch (TimeoutException) { }
        _cts.Dispose();
    }
}
