using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using System.Collections.Concurrent;
using System.Data;
using System.Text.Json;
using TownSuite.WorkQueues.Postgres;

namespace TownSuite.WorkQueues.Postgres;

/// <summary>
/// A PostgreSQL‑backed message bus. It handles publishing messages by inserting them into a database
/// and continuously polls for unprocessed messages to dispatch them to in‑memory subscribers.
/// </summary>
public class PostgresMessageBus : IMessageBus, IDisposable
{
    private readonly string _connectionString;
    private readonly CancellationTokenSource _cts = new CancellationTokenSource();
    private readonly ConcurrentDictionary<Type, ConcurrentBag<Func<object, Task>>> _handlers
        = new ConcurrentDictionary<Type, ConcurrentBag<Func<object, Task>>>();
    private readonly Task _pollingTask;
    private readonly ILogger _logger;
    private readonly SqlTransportOptions _options;

    public PostgresMessageBus(SqlTransportOptions options, 
        ILogger logger)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _connectionString = _options.ConnectionString;      
        // Start background polling for messages stored in PostgreSQL.
        _pollingTask = Task.Run(ProcessMessagesAsync);
    }

    /// <summary>
    /// Registers a consumer for a specific message type.
    /// </summary>
    public void Subscribe<T>(IConsumer<T> consumer)
    {
        var bag = _handlers.GetOrAdd(typeof(T), _ => new ConcurrentBag<Func<object, Task>>());
        Func<object, Task> handler = async (obj) =>
        {
            if (obj is T message)
            {
                var context = new SimpleConsumeContext<T>(message);
                await consumer.Consume(context);
            }
        };
        bag.Add(handler);
    }

    /// <summary>
    /// Publishes a message by serializing it to JSON and inserting it into the PostgreSQL table.
    /// </summary>
    public async Task Publish<T>(T message)
    {
        var messageType = typeof(T).FullName;
        var payload = JsonSerializer.Serialize(message);
        using (var conn = new NpgsqlConnection(_connectionString))
        {
            await conn.OpenAsync();
            var sql = $"INSERT INTO {_options.Schema}.workqueue(channel, payload) VALUES(@p_channel, @p_payload)";
            using (var cmd = new NpgsqlCommand(sql, conn))
            {
                cmd.Parameters.AddWithValue("@p_channel", messageType);
                cmd.Parameters.AddWithValue("@p_payload", payload);
                await cmd.ExecuteNonQueryAsync();
            }
        }
    }

    /// <summary>
    /// The background loop that polls PostgreSQL for unprocessed messages,
    /// claims them (using SELECT ... FOR UPDATE SKIP LOCKED), marks them as processed,
    /// and dispatches each message.
    /// </summary>
    private async Task ProcessMessagesAsync()
    {
        while (!_cts.Token.IsCancellationRequested)
        {
            try
            {
                int processedCount = await ClaimMessagesAsync(_options.MaxBatchSize);

                if (processedCount == 0)
                {
                    // Pause briefly when there are no messages.
                    await Task.Delay(_options.MaxWaitTime, _cts.Token);
                }
            }
            catch (TaskCanceledException)
            {
                // Graceful termination.
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error processing messages in PostgresMessageBus");
                await Task.Delay(1000, _cts.Token);
            }
        }
    }

    /// <summary>
    /// Retrieves a batch of unprocessed messages from the database using row locking.
    /// Marks each message as processed within the same transaction.
    /// </summary>
    private async Task<int> ClaimMessagesAsync(int maxMessages)
    {
        var messages = new List<MessageDto>();

        using (var conn = new NpgsqlConnection(_connectionString))
        {
            await conn.OpenAsync();
            using (var tran = await conn.BeginTransactionAsync())
            {
                // Select messages not yet processed and lock the rows.
                // filter for only channels that have registered handlers.
                if (_handlers.Count == 0)
                {
                    // No handlers registered, skip processing.
                    return 0;
                }
                var channels = string.Join(",", _handlers.Keys.Select(t => $"'{t.FullName}'"));
                if (string.IsNullOrEmpty(channels))
                {
                    // No channels to process, skip.
                    return 0;
                }
                // Use FOR UPDATE SKIP LOCKED to avoid blocking other transactions.
                // This allows us to claim messages without waiting for others to finish processing.
                // We also limit the number of messages to process in one go.
                // This is important to avoid overwhelming the system with too many messages at once.
                // The messages are ordered by creation time to process older messages first.
                // Note: Ensure the 'workqueue' table has the necessary indexes for performance.
                // The 'timecreatedutc' column should be indexed for efficient ordering.
                if (_logger?.IsEnabled(LogLevel.Debug) == true)
                {
                    _logger.LogDebug("Claiming up to {MaxMessages} messages from workqueue", maxMessages);
                }
                // Use parameterized query to prevent SQL injection and improve performance.
                if (_logger?.IsEnabled(LogLevel.Debug) == true)
                {
                    _logger.LogDebug("Using channels: {Channels}", channels);
                }
                // Use a parameterized query to avoid SQL injection and improve performance.
                // The 'timeprocessedutc' column is used to filter out already processed messages.
                // The 'FOR UPDATE SKIP LOCKED' clause allows us to lock the rows we are processing
                // without blocking other transactions that might be trying to process the same messages.
                // The 'LIMIT' clause ensures we only process a limited number of messages at a time.
                // This is important to avoid overwhelming the system with too many messages at once.
                // The 'ORDER BY timecreatedutc' clause ensures we process messages in the order they were created.
                // This is important to ensure that messages are processed in the order they were created,
                // which is important for many applications.

                var sql = @$"SELECT id, timecreatedutc, channel, payload, timeprocessedutc
                    FROM {_options.Schema}.workqueue WHERE timeprocessedutc is null 
                    and channel = ANY(@channels::text[])
                    ORDER BY timecreatedutc FOR UPDATE SKIP LOCKED LIMIT @maxMessages";
                using var cmd = new NpgsqlCommand(sql, conn, tran);
                cmd.Parameters.AddWithValue("@channels", channels);
                cmd.Parameters.AddWithValue("@maxMessages", maxMessages);
                using var reader = await cmd.ExecuteReaderAsync();

                while (await reader.ReadAsync())
                {
                    messages.Add(new MessageDto
                    {
                        Id = reader.GetInt32(0),
                        TimeCreatedUtc = reader.GetDateTime(1),
                        Channel = reader.GetString(2),
                        Payload = reader.GetString(3),
                        TimeProcessedUtc = reader.GetDateTime(4)
                    });
                }


                // Mark each claimed message as processed.
                foreach (var msg in messages)
                {
                    await DispatchMessageAsync(msg);

                    using (var updateCmd = new NpgsqlCommand($"UPDATE {_options.Schema}.workqueue SET timeprocessedutc = CURRENT_TIMESTAMP WHERE id = @id", conn, tran))
                    {
                        updateCmd.Parameters.AddWithValue("id", msg.Id);
                        await updateCmd.ExecuteNonQueryAsync();
                    }
                }
                await tran.CommitAsync();
            }
        }
        return messages.Count;
    }

    /// <summary>
    /// Dispatches a message to in‑memory consumers by deserializing its payload
    /// and invoking the registered handlers.
    /// </summary>
    private async Task DispatchMessageAsync(MessageDto msg)
    {
        // Locate the Channel using the stored message type name.
        var type = Type.GetType(msg.Channel);
        if (type == null)
        {
            _logger?.LogWarning("Unknown message channel: {Channel}", msg.Channel);
            return;
        }

        if (_handlers.TryGetValue(type, out var handlers))
        {
            // Deserialize the JSON payload back into the proper type.
            var message = JsonSerializer.Deserialize(msg.Payload, type);
            var tasks = handlers.Select(handler => handler(message));
            await Task.WhenAll(tasks);
        }
        else
        {
            _logger?.LogWarning("No handlers registered for message channel: {Channel}", msg.Channel);
        }
    }

    public void Dispose()
    {
        _cts.Cancel();
        try
        {
            _pollingTask?.Wait();
        }
        catch (AggregateException) { }
        _cts.Dispose();
    }

}