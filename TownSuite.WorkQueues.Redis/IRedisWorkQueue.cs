namespace TownSuite.WorkQueues.Redis;

/// <summary>
/// A simple Redis-backed FIFO work queue using Redis Lists (LPUSH / RPOP).
/// Unlike <see cref="IWorkQueue"/>, no database connection is required.
/// </summary>
public interface IRedisWorkQueue
{
    /// <summary>Enqueues <paramref name="payload"/> onto the named <paramref name="channel"/>.</summary>
    Task EnqueueAsync<T>(string channel, T payload);

    /// <summary>
    /// Dequeues and returns the next item from <paramref name="channel"/>,
    /// or <c>null</c> if the channel is empty.
    /// </summary>
    Task<T?> DequeueAsync<T>(string channel);
}
