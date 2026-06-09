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
}
