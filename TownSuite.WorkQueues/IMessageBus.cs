namespace TownSuite.WorkQueues;

/// <summary>
/// A lightweight publish/subscribe message bus.
/// Messages are persisted to the workqueue table and dispatched asynchronously
/// by a background polling loop. Each subscriber receives every message published
/// to its registered type.
/// </summary>
public interface IMessageBus
{
    /// <summary>
    /// Serialises <paramref name="message"/> and inserts it into the workqueue table.
    /// Delivery to subscribers happens asynchronously on the next polling cycle.
    /// </summary>
    /// <typeparam name="T">The message type. <c>typeof(T).FullName</c> is used as the channel name
    /// and must not exceed 500 characters.</typeparam>
    /// <param name="message">The message to publish.</param>
    Task Publish<T>(T message);

    /// <summary>
    /// Registers <paramref name="consumer"/> to receive messages of type <typeparamref name="T"/>.
    /// Multiple consumers may be subscribed to the same type; each receives an independent copy.
    /// Subscribers must be registered before the bus begins delivering messages — call this
    /// before the first <see cref="Publish{T}"/> or immediately after construction.
    /// </summary>
    /// <typeparam name="T">The message type to subscribe to.</typeparam>
    /// <param name="consumer">The consumer implementation.</param>
    void Subscribe<T>(IConsumer<T> consumer);
}
