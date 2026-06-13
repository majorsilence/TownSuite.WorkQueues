namespace TownSuite.WorkQueues
{
    /// <summary>
    /// Shared configuration for the polling behaviour common to all message bus backends
    /// (PostgreSQL, SQL Server, Redis). All three transport options classes inherit from this.
    /// </summary>
    public class BatchOptions
    {
        /// <summary>
        /// The maximum number of messages to process in a single batch.
        /// </summary>
        public int MaxBatchSize { get; set; } = 100;
        /// <summary>
        /// The maximum time to wait for messages before processing the batch.
        /// </summary>
        public TimeSpan MaxWaitTime { get; set; } = TimeSpan.FromSeconds(5);
        /// <summary>
        /// Poll continuously without delay even when no messages are available.
        /// When <see langword="false"/> (the default) the bus waits <see cref="MaxWaitTime"/> between
        /// empty polls, which reduces CPU and database load. Set to <see langword="true"/> only
        /// in tests or latency-critical scenarios.
        /// </summary>
        public bool ContinuousPolling { get; set; } = false;

        /// <summary>Whether to allow empty batches (i.e., processing when no messages are available).</summary>
        [Obsolete("Use ContinuousPolling instead. AllowEmptyBatches will be removed in a future version.")]
        public bool AllowEmptyBatches { get => ContinuousPolling; set => ContinuousPolling = value; }

        /// <summary>
        /// Maximum number of delivery attempts before a message is moved to the dead-letter state.
        /// </summary>
        public int MaxRetries { get; set; } = 3;

        /// <summary>
        /// Minimum delay between retry attempts. Defaults to <see cref="TimeSpan.Zero"/> (retry
        /// immediately on the next polling cycle). For SQL-backed transports a non-zero value sets
        /// the <c>scheduledfor</c> column to <c>NOW() + RetryDelay</c> so the message is withheld
        /// from polling until the delay elapses.
        /// Redis transports use <c>RedisOptions.ReclaimIdleTime</c> to control retry timing instead.
        /// </summary>
        public TimeSpan RetryDelay { get; set; } = TimeSpan.Zero;
    }
}
