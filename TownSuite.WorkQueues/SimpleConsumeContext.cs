namespace TownSuite.WorkQueues;

/// <summary>
/// A basic implementation of the consume context.
/// </summary>
/// <typeparam name="T">The message type.</typeparam>
public class SimpleConsumeContext<T> : ConsumeContext<T>
{
    public T Message { get; private set; }

    public SimpleConsumeContext(T message)
    {
        Message = message;
    }
}

