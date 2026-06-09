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

    /// <param name="message">The message payload to expose via <see cref="Message"/>.</param>
    public SimpleConsumeContext(T message)
    {
        Message = message;
    }
}
