namespace TownSuite.WorkQueues;

/// <summary>
/// A lightweight publish/subscribe message bus.
/// Messages are persisted to the backing store and dispatched asynchronously
/// by a background polling loop. Each subscriber receives every message published
/// to its registered type.
/// </summary>
/// <remarks>
/// <para><strong>Delivery guarantee:</strong> at-least-once. A message is retried up to
/// <c>BatchOptions.MaxRetries</c> times on failure, then dead-lettered.</para>
/// <para><strong>Multiple consumers on the same type:</strong> all registered consumers receive
/// the same message in a single dispatch. If any consumer throws, the entire message is retried —
/// including consumers that succeeded in the previous attempt. Design consumers to be idempotent.</para>
/// <para><strong>Lifecycle:</strong> dispose the bus (via <see cref="IAsyncDisposable.DisposeAsync"/>)
/// to stop the polling loop gracefully before application shutdown.</para>
/// </remarks>
public interface IMessageBus : IAsyncDisposable
{
    /// <summary>
    /// Serialises <paramref name="message"/> and inserts it into the backing store.
    /// Delivery to subscribers happens asynchronously on the next polling cycle.
    /// </summary>
    /// <typeparam name="T">The message type. <c>typeof(T).FullName</c> is used as the channel name
    /// and must not exceed 500 characters.</typeparam>
    /// <param name="message">The message to publish.</param>
    /// <param name="cancellationToken">Token to cancel the publish operation.</param>
    Task Publish<T>(T message, CancellationToken cancellationToken = default);

    /// <summary>
    /// Registers <paramref name="consumer"/> to receive messages of type <typeparamref name="T"/>.
    /// Multiple consumers may be subscribed to the same type; each receives every message.
    /// Subscribe before the bus starts delivering — call this immediately after construction
    /// and before the first <see cref="Publish{T}"/>.
    /// </summary>
    /// <typeparam name="T">The message type to subscribe to.</typeparam>
    /// <param name="consumer">The consumer implementation.</param>
    void Subscribe<T>(IConsumer<T> consumer);

    /// <summary>
    /// Resets all dead-lettered messages of type <typeparamref name="T"/> so they will be
    /// redelivered on the next polling cycle. Returns the number of messages replayed.
    /// </summary>
    /// <typeparam name="T">The message type whose dead-letter queue should be replayed.</typeparam>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    Task<int> ReplayDeadLettered<T>(CancellationToken cancellationToken = default);

    /// <summary>
    /// <see langword="true"/> while the background polling loop is running.
    /// Use this to implement health checks: a <see langword="false"/> value after startup
    /// indicates the loop has stopped unexpectedly and the bus is no longer processing messages.
    /// </summary>
    bool IsPolling { get; }
}
