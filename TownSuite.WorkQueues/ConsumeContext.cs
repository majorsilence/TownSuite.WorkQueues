namespace TownSuite.WorkQueues;

/// <summary>
/// Delivers a message to a consumer along with metadata about the message envelope.
/// </summary>
/// <typeparam name="T">The message type.</typeparam>
public interface ConsumeContext<T>
{
    /// <summary>Gets the deserialised message payload.</summary>
    T Message { get; }

    /// <summary>
    /// A token that is cancelled when the bus is shutting down. Consumers doing I/O
    /// should pass this to async calls so they can terminate gracefully.
    /// </summary>
    CancellationToken CancellationToken => CancellationToken.None;

    /// <summary>
    /// Unique identifier assigned to this message when it was published.
    /// Returns <see cref="Guid.Empty"/> for transports or implementations that do not populate this value.
    /// Use this for idempotency checks.
    /// </summary>
    Guid MessageId => Guid.Empty;

    /// <summary>
    /// UTC timestamp at which this message was published.
    /// Returns <see cref="DateTimeOffset.MinValue"/> for transports or implementations that do not populate this value.
    /// </summary>
    DateTimeOffset SentTime => DateTimeOffset.MinValue;
}
