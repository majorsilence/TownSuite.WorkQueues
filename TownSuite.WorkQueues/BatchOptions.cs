namespace TownSuite.WorkQueues
{
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
        /// Whether to allow empty batches (i.e., processing when no messages are available).
        /// </summary>
        public bool AllowEmptyBatches { get; set; } = false;

        /// <summary>
        /// Maximum number of delivery attempts before a message is moved to the dead-letter state.
        /// </summary>
        public int MaxRetries { get; set; } = 3;
    }
}
