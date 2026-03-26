namespace TownSuite.WorkQueues;

/// <summary>
/// Provides context for a message. This can be extended with headers, correlation IDs, etc.
/// </summary>
/// <typeparam name="T">The message type.</typeparam>
public interface ConsumeContext<T>
{
    T Message { get; }
}

