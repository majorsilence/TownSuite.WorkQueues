namespace TownSuite.WorkQueues;

/// <summary>
/// Thrown when a WorkQueues operation cannot proceed due to an invalid argument or
/// unsupported runtime state (e.g. a missing transaction, an incompatible connection type).
/// </summary>
public class WorkQueuesException : Exception
{
    /// <param name="message">A description of why the operation failed.</param>
    public WorkQueuesException(string message) : base(message) { }

    /// <param name="message">A description of why the operation failed.</param>
    /// <param name="ex">The underlying exception, if any.</param>
    public WorkQueuesException(string message, Exception ex) : base(message, ex) { }
}
