namespace TownSuite.WorkQueues;

/// <summary>
/// Default <see cref="ConsumeContext{T}"/> implementation used by the built-in message bus dispatcher.
/// Useful when writing unit tests for <see cref="IConsumer{T}"/> implementations:
/// <code>
/// var ctx = new SimpleConsumeContext&lt;MyMessage&gt;(new MyMessage { ... });
/// await consumer.Consume(ctx);
/// </code>
/// </summary>
/// <typeparam name="T">The message type.</typeparam>
public class SimpleConsumeContext<T> : ConsumeContext<T>
{
    /// <inheritdoc/>
    public T Message { get; private set; }

    /// <inheritdoc/>
    public CancellationToken CancellationToken { get; }

    /// <param name="message">The message payload to expose via <see cref="Message"/>.</param>
    /// <param name="cancellationToken">Bus shutdown token forwarded to the consumer.</param>
    public SimpleConsumeContext(T message, CancellationToken cancellationToken = default)
    {
        Message = message;
        CancellationToken = cancellationToken;
    }
}
