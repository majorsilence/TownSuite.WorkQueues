using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using System.Collections.Concurrent;
using System.Text.Json;

namespace TownSuite.WorkQueues.Postgres;

public class PostgresMessageBus : IMessageBus
{
    private readonly CancellationTokenSource _cts = new();
    private readonly ConcurrentDictionary<Type, ConcurrentDictionary<object, Func<object, Guid, DateTimeOffset, Task>>> _handlers = new();
    private readonly ConcurrentDictionary<Type, ConcurrentDictionary<object, Func<object, Task>>> _faultHandlers = new();
    private readonly ConcurrentDictionary<Type, Func<string, Exception, int, Task>> _faultDispatchers = new();
    private readonly Task _pollingTask;
    private readonly ILogger _logger;
    private readonly SqlTransportOptions _options;
    private readonly IServiceProvider? _serviceProvider;

    public PostgresMessageBus(SqlTransportOptions options, ILogger logger, IServiceProvider? serviceProvider = null)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _logger  = logger  ?? throw new ArgumentNullException(nameof(logger));
        _serviceProvider = serviceProvider;
        // Yield to the caller so Subscribe() calls made immediately after construction
        // are registered before the first poll cycle runs.
        _pollingTask = Task.Run(async () => { await Task.Yield(); await ProcessMessagesAsync(); });
    }

    /// <inheritdoc />
    public bool IsPolling => !_pollingTask.IsCompleted && !_pollingTask.IsFaulted;

    /// <inheritdoc />
    public void Subscribe<T>(IConsumer<T> consumer)
    {
        var handlers = _handlers.GetOrAdd(typeof(T),
            _ => new ConcurrentDictionary<object, Func<object, Guid, DateTimeOffset, Task>>());
        handlers.TryAdd(consumer, async (obj, messageId, sentTime) =>
        {
            if (obj is T message)
                await consumer.Consume(new SimpleConsumeContext<T>(message, _cts.Token, messageId, sentTime));
        });
        EnsureFaultDispatcher<T>();
    }

    /// <summary>
    /// Registers a scoped consumer resolved fresh from an <see cref="IServiceScope"/> on every
    /// message dispatch. Requires <see cref="IServiceProvider"/> to have been passed to the
    /// constructor (automatically supplied by <see cref="PostgresMigrationHostedServiceExtensions.AddPostgresMessageBus"/>).
    /// </summary>
    public void Subscribe<TMessage, TConsumer>() where TConsumer : class, IConsumer<TMessage>
    {
        if (_serviceProvider == null)
            throw new InvalidOperationException(
                "Scoped consumer registration requires IServiceProvider. " +
                "Pass serviceProvider to the PostgresMessageBus constructor, " +
                "or use the AddPostgresMessageBus DI extension.");

        var handlers = _handlers.GetOrAdd(typeof(TMessage),
            _ => new ConcurrentDictionary<object, Func<object, Guid, DateTimeOffset, Task>>());
        handlers.TryAdd(typeof(TConsumer), async (obj, messageId, sentTime) =>
        {
            if (obj is TMessage message)
            {
                await using var scope = _serviceProvider.CreateAsyncScope();
                var consumer = scope.ServiceProvider.GetRequiredService<TConsumer>();
                await consumer.Consume(new SimpleConsumeContext<TMessage>(message, _cts.Token, messageId, sentTime));
            }
        });
        EnsureFaultDispatcher<TMessage>();
    }

    /// <inheritdoc />
    public void SubscribeFault<T>(IConsumer<Fault<T>> consumer)
    {
        var handlers = _faultHandlers.GetOrAdd(typeof(T),
            _ => new ConcurrentDictionary<object, Func<object, Task>>());
        handlers.TryAdd(consumer, async obj =>
        {
            if (obj is Fault<T> fault)
                await consumer.Consume(new SimpleConsumeContext<Fault<T>>(fault, _cts.Token));
        });
    }

    private void EnsureFaultDispatcher<T>()
    {
        _faultDispatchers.TryAdd(typeof(T), async (payload, ex, attemptCount) =>
        {
            if (!_faultHandlers.TryGetValue(typeof(T), out var handlers) || handlers.IsEmpty)
                return;

            var original = LegacyJsonDeserializer.Deserialize(payload, typeof(T));
            if (original is not T typedOriginal) return;

            var fault = new Fault<T>
            {
                OriginalMessage  = typedOriginal,
                ExceptionType    = ex.GetType().FullName ?? ex.GetType().Name,
                ExceptionMessage = ex.Message,
                StackTrace       = ex.StackTrace,
                FaultedAt        = DateTimeOffset.UtcNow,
                AttemptCount     = attemptCount
            };

            await Task.WhenAll(handlers.Values.Select(h => h(fault)));
        });
    }

    /// <inheritdoc />
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

    /// <inheritdoc />
    public async Task Publish<T>(T message, DateTimeOffset deliverAfter, CancellationToken cancellationToken = default)
    {
        var channel = typeof(T).FullName
            ?? throw new InvalidOperationException($"Cannot determine channel name for type {typeof(T)}");

        if (channel.Length > 500)
            throw new ArgumentException($"Message type name exceeds 500 characters: {channel}");

        var payload = JsonSerializer.Serialize(message);
        await using var conn = new NpgsqlConnection(_options.ConnectionString);
        await conn.OpenAsync(cancellationToken);
        var sql = $"INSERT INTO {_options.Schema}.workqueue(channel, payload, scheduledfor) VALUES(@channel, @payload, @scheduledfor)";
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@channel", channel);
        cmd.Parameters.AddWithValue("@payload", payload);
        cmd.Parameters.Add(new NpgsqlParameter("@scheduledfor", NpgsqlTypes.NpgsqlDbType.Timestamp)
        {
            Value = DateTime.SpecifyKind(deliverAfter.UtcDateTime, DateTimeKind.Unspecified)
        });
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <inheritdoc />
    public async Task<int> ReplayDeadLettered<T>(CancellationToken cancellationToken = default)
    {
        var channel = typeof(T).FullName
            ?? throw new InvalidOperationException($"Cannot determine channel name for type {typeof(T)}");

        await using var conn = new NpgsqlConnection(_options.ConnectionString);
        await conn.OpenAsync(cancellationToken);
        var sql = $"""
            UPDATE {_options.Schema}.workqueue
            SET failedat = NULL, retrycount = 0, scheduledfor = NULL
            WHERE channel = @channel AND failedat IS NOT NULL
            """;
        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@channel", channel);
        return await cmd.ExecuteNonQueryAsync(cancellationToken);
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
        if (!_options.ContinuousPolling)
            await Task.Delay(_options.MaxWaitTime, _cts.Token);
    }

    private async Task<int> ClaimMessagesAsync(int maxMessages)
    {
        if (_handlers.Count == 0) return 0;

        var channelNames = _handlers.Keys
            .Select(t => t.FullName)
            .OfType<string>()
            .ToArray();

        if (channelNames.Length == 0) return 0;

        var messages = new List<MessageDto>();

        await using var conn = new NpgsqlConnection(_options.ConnectionString);
        await conn.OpenAsync();
        await using var tran = await conn.BeginTransactionAsync();

        var selectSql = $"""
            SELECT id, channel, payload, retrycount, messageid, timecreatedutc
            FROM {_options.Schema}.workqueue
            WHERE timeprocessedutc IS NULL
              AND failedat IS NULL
              AND (scheduledfor IS NULL OR scheduledfor <= CURRENT_TIMESTAMP)
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
                    Id             = reader.GetInt32(0),
                    Channel        = reader.GetString(1),
                    Payload        = reader.GetString(2),
                    RetryCount     = reader.GetInt32(3),
                    MessageId      = reader.GetGuid(4),
                    TimeCreatedUtc = reader.GetDateTime(5)
                });
            }
        }

        foreach (var msg in messages)
        {
            bool success = false;
            Exception? lastException = null;
            try
            {
                await DispatchMessageAsync(msg);
                success = true;
            }
            catch (Exception ex)
            {
                lastException = ex;
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
                bool willDeadLetter = msg.RetryCount + 1 >= _options.MaxRetries;

                // When a retry delay is configured and the message won't be dead-lettered, hold it back.
                DateTime? scheduledFor = !willDeadLetter && _options.RetryDelay > TimeSpan.Zero
                    ? DateTime.SpecifyKind(DateTime.UtcNow.Add(_options.RetryDelay), DateTimeKind.Unspecified)
                    : (DateTime?)null;

                await using var retryCmd = new NpgsqlCommand($"""
                    UPDATE {_options.Schema}.workqueue
                    SET retrycount = retrycount + 1,
                        failedat = CASE WHEN retrycount + 1 >= @maxRetries THEN CURRENT_TIMESTAMP ELSE NULL END,
                        scheduledfor = @scheduledFor
                    WHERE id = @id
                    """, conn, tran);
                retryCmd.Parameters.AddWithValue("id", msg.Id);
                retryCmd.Parameters.AddWithValue("maxRetries", _options.MaxRetries);
                retryCmd.Parameters.Add(new NpgsqlParameter("scheduledFor", NpgsqlTypes.NpgsqlDbType.Timestamp)
                {
                    Value = scheduledFor.HasValue ? (object)scheduledFor.Value : DBNull.Value
                });
                await retryCmd.ExecuteNonQueryAsync();

                if (willDeadLetter && lastException != null)
                {
                    var msgType = _handlers.Keys.FirstOrDefault(t => t.FullName == msg.Channel);
                    if (msgType != null && _faultDispatchers.TryGetValue(msgType, out var dispatcher))
                    {
                        try { await dispatcher(msg.Payload, lastException, msg.RetryCount + 1); }
                        catch (Exception fex)
                        {
                            _logger.LogError(fex, "Fault consumer threw for dead-lettered message {Id}", msg.Id);
                        }
                    }
                }
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
            _logger.LogWarning("No handlers registered for channel: {Channel} — message {Id} will be skipped", msg.Channel, msg.Id);
            return;
        }

        if (!_handlers.TryGetValue(type, out var handlers)) return;

        var message = LegacyJsonDeserializer.Deserialize(msg.Payload, type);
        var sentTime = new DateTimeOffset(msg.TimeCreatedUtc, TimeSpan.Zero);
        await Task.WhenAll(handlers.Values.Select(h => h(message!, msg.MessageId, sentTime)));
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
