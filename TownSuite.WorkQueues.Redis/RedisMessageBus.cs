using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using System.Collections.Concurrent;
using System.Text.Json;

namespace TownSuite.WorkQueues.Redis;

/// <summary>
/// Redis Streams-backed message bus with at-least-once delivery, consumer groups,
/// automatic retry, and dead-lettering.
/// </summary>
public class RedisMessageBus : IMessageBus
{
    private readonly CancellationTokenSource _cts = new();
    private readonly ConcurrentDictionary<Type, ConcurrentDictionary<object, Func<object, Task>>> _handlers = new();
    private readonly ConcurrentDictionary<string, bool> _groupsEnsured = new();
    private readonly Task _pollingTask;
    private readonly ILogger _logger;
    private readonly IConnectionMultiplexer _redis;
    private readonly RedisOptions _options;

    public RedisMessageBus(IConnectionMultiplexer redis, RedisOptions options, ILogger logger)
    {
        _redis = redis ?? throw new ArgumentNullException(nameof(redis));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        // Yield to the caller so Subscribe() calls made immediately after construction
        // are registered before the first poll cycle runs.
        _pollingTask = Task.Run(async () => { await Task.Yield(); await ProcessMessagesAsync(); });
    }

    /// <inheritdoc />
    public bool IsPolling => !_pollingTask.IsCompleted && !_pollingTask.IsFaulted;

    /// <inheritdoc />
    public void Subscribe<T>(IConsumer<T> consumer)
    {
        var handlers = _handlers.GetOrAdd(typeof(T), _ => new ConcurrentDictionary<object, Func<object, Task>>());
        handlers.TryAdd(consumer, async obj =>
        {
            if (obj is T message)
                await consumer.Consume(new SimpleConsumeContext<T>(message, _cts.Token));
        });
    }

    /// <inheritdoc />
    public async Task Publish<T>(T message, CancellationToken cancellationToken = default)
    {
        var channel = typeof(T).FullName
            ?? throw new InvalidOperationException($"Cannot determine channel name for type {typeof(T)}");

        if (channel.Length > 500)
            throw new ArgumentException($"Message type name exceeds 500 characters: {channel}");

        cancellationToken.ThrowIfCancellationRequested();
        var payload = JsonSerializer.Serialize(message);
        var db = _redis.GetDatabase();
        await db.StreamAddAsync(StreamKey(channel), [new NameValueEntry("payload", payload)]);
    }

    /// <inheritdoc />
    public async Task<int> ReplayDeadLettered<T>(CancellationToken cancellationToken = default)
    {
        var channel = typeof(T).FullName
            ?? throw new InvalidOperationException($"Cannot determine channel name for type {typeof(T)}");

        var streamKey = StreamKey(channel);
        var deadKey = $"{streamKey}:dead";
        var db = _redis.GetDatabase();

        int replayed = 0;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var entries = await db.StreamReadAsync(deadKey, "0-0", count: 100);
            if (entries == null || entries.Length == 0) break;

            foreach (var entry in entries)
            {
                await db.StreamAddAsync(streamKey, entry.Values);
                await db.StreamDeleteAsync(deadKey, new[] { entry.Id });
                replayed++;
            }

            if (entries.Length < 100) break;
        }

        return replayed;
    }

    private async Task ProcessMessagesAsync()
    {
        while (!_cts.Token.IsCancellationRequested)
        {
            try
            {
                int processed = 0;
                var db = _redis.GetDatabase();

                foreach (var (type, _) in _handlers)
                {
                    var channel = type.FullName;
                    if (channel == null) continue;
                    processed += await ProcessStreamAsync(db, StreamKey(channel), channel);
                }

                if (processed == 0)
                    await WaitAsync();
            }
            catch (TaskCanceledException) { }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in RedisMessageBus polling loop");
                try { await Task.Delay(1000, _cts.Token); } catch (TaskCanceledException) { }
            }
        }
    }

    private async Task WaitAsync()
    {
        if (!_options.ContinuousPolling)
            await Task.Delay(_options.MaxWaitTime, _cts.Token);
    }

    private async Task EnsureConsumerGroupAsync(IDatabase db, string streamKey)
    {
        if (_groupsEnsured.ContainsKey(streamKey)) return;

        try
        {
            await db.StreamCreateConsumerGroupAsync(
                streamKey,
                _options.ConsumerGroup,
                StreamPosition.Beginning,
                createStream: true);
        }
        catch (RedisServerException ex) when (ex.Message.StartsWith("BUSYGROUP"))
        {
            // Group already exists — this is normal on restart
        }

        _groupsEnsured.TryAdd(streamKey, true);
    }

    private async Task<int> ProcessStreamAsync(IDatabase db, string streamKey, string typeName)
    {
        await EnsureConsumerGroupAsync(db, streamKey);

        int processed = 0;

        // Claim new, undelivered messages
        var newEntries = await db.StreamReadGroupAsync(
            streamKey,
            _options.ConsumerGroup,
            _options.ConsumerName,
            ">",
            count: _options.MaxBatchSize);

        foreach (var entry in newEntries)
        {
            bool success = await DispatchEntryAsync(entry, typeName);
            if (success)
            {
                await db.StreamAcknowledgeAsync(streamKey, _options.ConsumerGroup, entry.Id);
                processed++;
            }
            // On failure: message stays in the Pending Entry List; XAUTOCLAIM reclaims it later
        }

        // Reclaim stale pending messages (retries)
        StreamAutoClaimResult claimed;
        try
        {
            claimed = await db.StreamAutoClaimAsync(
                streamKey,
                _options.ConsumerGroup,
                _options.ConsumerName,
                _options.ReclaimIdleTimeMs,
                "0-0",
                count: _options.MaxBatchSize);
        }
        catch (RedisServerException)
        {
            // XAUTOCLAIM unavailable (Redis < 6.2) — skip retry logic
            return processed;
        }

        if (claimed.ClaimedEntries.Length == 0)
            return processed;

        var retryHashKey = $"{_options.KeyPrefix}:retries";

        foreach (var entry in claimed.ClaimedEntries)
        {
            var msgIdField = entry.Id.ToString();
            var retryCount = (int)await db.HashIncrementAsync(retryHashKey, msgIdField);

            if (retryCount >= _options.MaxRetries)
            {
                // Dead-letter: copy the entry to a dead-letter stream then ACK
                await db.StreamAddAsync($"{streamKey}:dead", entry.Values);
                await db.StreamAcknowledgeAsync(streamKey, _options.ConsumerGroup, entry.Id);
                await db.HashDeleteAsync(retryHashKey, msgIdField);
                _logger.LogWarning(
                    "Message {Id} dead-lettered on stream {Stream} after {Count} retries",
                    entry.Id, streamKey, retryCount);
                continue;
            }

            bool success = await DispatchEntryAsync(entry, typeName);
            if (success)
            {
                await db.StreamAcknowledgeAsync(streamKey, _options.ConsumerGroup, entry.Id);
                await db.HashDeleteAsync(retryHashKey, msgIdField);
                processed++;
            }
        }

        return processed;
    }

    private async Task<bool> DispatchEntryAsync(StreamEntry entry, string typeName)
    {
        try
        {
            var payloadField = entry.Values.FirstOrDefault(v => v.Name == "payload");
            if (payloadField.Value.IsNull)
            {
                _logger.LogWarning("Stream entry {Id} has no 'payload' field — ACK-ing to avoid infinite retry", entry.Id);
                return true;
            }

            var type = _handlers.Keys.FirstOrDefault(t => t.FullName == typeName);
            if (type == null || !_handlers.TryGetValue(type, out var handlers))
            {
                _logger.LogWarning("No handler registered for channel {Channel} — ACK-ing", typeName);
                return true;
            }

            var message = LegacyJsonDeserializer.Deserialize((string)payloadField.Value!, type);
            if (message == null) return true;

            await Task.WhenAll(handlers.Values.Select(h => h(message)));
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Handler failed for stream entry {Id} on channel {Channel}", entry.Id, typeName);
            return false;
        }
    }

    private string StreamKey(string channel) => $"{_options.KeyPrefix}:stream:{channel}";

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        try { await _pollingTask.WaitAsync(TimeSpan.FromSeconds(10)); }
        catch (OperationCanceledException) { }
        catch (TimeoutException) { }
        _cts.Dispose();
    }
}
