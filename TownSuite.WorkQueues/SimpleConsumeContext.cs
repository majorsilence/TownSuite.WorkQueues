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

    /// <inheritdoc/>
    public Guid MessageId { get; }

    /// <inheritdoc/>
    public DateTimeOffset SentTime { get; }

    /// <param name="message">The message payload to expose via <see cref="Message"/>.</param>
    /// <param name="cancellationToken">Bus shutdown token forwarded to the consumer.</param>
    /// <param name="messageId">Unique message identifier from the backing store.</param>
    /// <param name="sentTime">UTC timestamp when the message was published.</param>
    public SimpleConsumeContext(T message, CancellationToken cancellationToken = default,
        Guid messageId = default, DateTimeOffset sentTime = default)
    {
        Message = message;
        CancellationToken = cancellationToken;
        MessageId = messageId;
        SentTime = sentTime;
    }
}
