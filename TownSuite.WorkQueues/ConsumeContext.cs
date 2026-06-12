namespace TownSuite.WorkQueues;

/// <summary>
/// Delivers a message to a consumer, with room to carry future metadata
/// (e.g. correlation IDs, headers, retry count).
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
}
