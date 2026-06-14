namespace TownSuite.WorkQueues.Sqlite;

/// <summary>Options for the SQLite-backed message bus and migration service.</summary>
public class SqliteTransportOptions : BatchOptions
{
    /// <summary>
    /// SQLite connection string. Example: <c>Data Source=./workqueue.db</c>
    /// </summary>
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>
    /// How long a claimed message is held before another process may reclaim it.
    /// If the claiming process crashes before completing, the message becomes
    /// available again after this interval elapses. Default: 60 seconds.
    /// </summary>
    /// <remarks>
    /// Set this to comfortably exceed the longest expected consumer processing time.
    /// A message whose consumer takes longer than <see cref="LockTimeout"/> may be
    /// delivered a second time while the first delivery is still in progress.
    /// </remarks>
    public TimeSpan LockTimeout { get; set; } = TimeSpan.FromSeconds(60);
}
