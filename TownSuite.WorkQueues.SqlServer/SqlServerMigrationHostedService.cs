using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace TownSuite.WorkQueues.SqlServer;

/// <summary>
/// Hosted service that applies idempotent DDL migrations for the SQL Server workqueue schema
/// on application startup. Safe to run against an existing database — all statements are
/// guarded with existence checks. Requires SQL Server 2016 or later.
/// </summary>
public class SqlServerMigrationHostedService : IHostedService
{
    private readonly IServiceProvider _sp;
    private readonly ILogger<SqlServerMigrationHostedService> _logger;

    public SqlServerMigrationHostedService(
        IServiceProvider sp,
        ILogger<SqlServerMigrationHostedService> logger)
    {
        _sp     = sp     ?? throw new ArgumentNullException(nameof(sp));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        using var scope   = _sp.CreateScope();
        var options = scope.ServiceProvider.GetRequiredService<SqlServerTransportOptions>();
        await MigrateAsync(options, cancellationToken);
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    private async Task MigrateAsync(SqlServerTransportOptions options, CancellationToken ct)
    {
        _logger.LogInformation(
            "Running SQL Server workqueue migrations for schema [{Schema}]", options.Schema);

        try
        {
            var scripts = new[]
            {
                Load("TownSuite.WorkQueues.SqlServer.sql.dbo.WorkQueue.sql"),
                Load("TownSuite.WorkQueues.SqlServer.sql.dbo.WorkQueue_Enqueue.sql"),
                Load("TownSuite.WorkQueues.SqlServer.sql.dbo.WorkQueue_Dequeue.sql"),
                Load("TownSuite.WorkQueues.SqlServer.sql.dbo.WorkQueue_Dequeue_NonDestructive.sql"),
            };

            await using var conn = new SqlConnection(options.AdminConnectionString);
            await conn.OpenAsync(ct);

            foreach (var script in scripts)
            {
                // Replace the default schema token so a non-dbo schema is fully supported.
                var sql = script
                    .Replace("[dbo].", $"[{options.Schema}].");

                foreach (var batch in SplitBatches(sql))
                {
                    await using var cmd = new SqlCommand(batch, conn);
                    cmd.CommandTimeout = 120;
                    await cmd.ExecuteNonQueryAsync(ct);
                }
            }

            _logger.LogInformation("SQL Server workqueue migrations completed.");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "SQL Server workqueue migration failed.");
            throw;
        }
    }

    private static string Load(string resourceName) =>
        EmbeddedSqlReader.GetEmbeddedSql(resourceName);

    /// <summary>
    /// Splits a T-SQL script into individual batches on GO statement boundaries.
    /// GO must appear on a line by itself (case-insensitive), matching SSMS behaviour.
    /// </summary>
    internal static IEnumerable<string> SplitBatches(string sql)
    {
        var lines   = sql.Split('\n');
        var current = new System.Text.StringBuilder();

        foreach (var line in lines)
        {
            if (line.TrimEnd('\r').Trim().Equals("GO", StringComparison.OrdinalIgnoreCase))
            {
                var batch = current.ToString().Trim();
                if (!string.IsNullOrWhiteSpace(batch))
                    yield return batch;
                current.Clear();
            }
            else
            {
                current.AppendLine(line);
            }
        }

        var remaining = current.ToString().Trim();
        if (!string.IsNullOrWhiteSpace(remaining))
            yield return remaining;
    }
}
