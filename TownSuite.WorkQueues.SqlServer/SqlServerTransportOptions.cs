namespace TownSuite.WorkQueues.SqlServer;

/// <summary>Options for the SQL Server-backed message bus and migration service.</summary>
public class SqlServerTransportOptions : BatchOptions
{
    /// <summary>Connection string used for message reads, writes, and publishing.</summary>
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>
    /// Connection string used by the migration hosted service to run DDL.
    /// May require broader permissions than <see cref="ConnectionString"/>.
    /// Defaults to <see cref="ConnectionString"/> when not set.
    /// </summary>
    public string AdminConnectionString
    {
        get => string.IsNullOrEmpty(_adminConnectionString) ? ConnectionString : _adminConnectionString;
        set => _adminConnectionString = value;
    }

    private string _adminConnectionString = string.Empty;

    /// <summary>
    /// Database schema that contains the workqueue table. Default: <c>"dbo"</c>.
    /// </summary>
    public string Schema { get; set; } = "dbo";
}
