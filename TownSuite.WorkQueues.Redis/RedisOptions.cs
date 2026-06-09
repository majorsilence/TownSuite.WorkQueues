namespace TownSuite.WorkQueues.Redis;

/// <summary>Options for the Redis-backed work queue and message bus.</summary>
public class RedisOptions : BatchOptions
{
    /// <summary>
    /// Prefix applied to all Redis keys created by this library. Default: <c>"workqueue"</c>.
    /// </summary>
    public string KeyPrefix { get; set; } = "workqueue";

    /// <summary>Consumer group name used by <see cref="RedisMessageBus"/>. Default: <c>"default"</c>.</summary>
    public string ConsumerGroup { get; set; } = "default";

    /// <summary>Consumer instance name within the group. Defaults to the machine name.</summary>
    public string ConsumerName { get; set; } = Environment.MachineName;

    /// <summary>
    /// How long a pending message must be idle before it is eligible for reclaim.
    /// Defaults to three times <see cref="BatchOptions.MaxWaitTime"/>.
    /// </summary>
    public TimeSpan? ReclaimIdleTime { get; set; }

    internal long ReclaimIdleTimeMs =>
        (long)(ReclaimIdleTime ?? MaxWaitTime * 3).TotalMilliseconds;
}
