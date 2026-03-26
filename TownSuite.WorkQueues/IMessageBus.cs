namespace TownSuite.WorkQueues;

/// <summary>
/// The core interface for a message bus.
/// </summary>
public interface IMessageBus
{
    /// <summary>
    /// Publish a message to all registered consumers.
    /// </summary>
    /// <typeparam name="T">The type of the message.</typeparam>
    /// <param name="message">The message payload.</param>
    Task Publish<T>(T message);

    /// <summary>
    /// Subscribe a consumer to a message type.
    /// </summary>
    /// <typeparam name="T">The message type.</typeparam>
    /// <param name="consumer">The consumer that handles messages of type T.</param>
    void Subscribe<T>(IConsumer<T> consumer);
}

