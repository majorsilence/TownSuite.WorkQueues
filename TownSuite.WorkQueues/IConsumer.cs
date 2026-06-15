namespace TownSuite.WorkQueues;

/// <summary>
/// Defines a consumer that processes messages of type T.
/// </summary>
/// <typeparam name="T">The message type.</typeparam>
public interface IConsumer<T>
{
    /// <summary>
    /// Process the message encapsulated in the consume context.
    /// </summary>
    /// <param name="context">The message context.</param>
    Task Consume(ConsumeContext<T> context);
}

