using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace TownSuite.WorkQueues.Postgres;

public class PostgresMigrationHostedService : IHostedService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<PostgresMigrationHostedService> _logger;

    public PostgresMigrationHostedService(IServiceProvider serviceProvider,
        ILogger<PostgresMigrationHostedService> logger)
    {
        _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        using (var scope = _serviceProvider.CreateScope())
        {
            var options = scope.ServiceProvider.GetRequiredService<SqlTransportOptions>();
            await MigrateDatabaseAsync(options, cancellationToken);
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    private async Task MigrateDatabaseAsync(SqlTransportOptions options, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Running migrations for PostgreSQL database with schema {Schema}", options.Schema);
        try
        {
            var workeQueueTable = EmbeddedSqlReader
                .GetEmbeddedSql("TownSuite.WorkQueues.Postgres.sql.public.WorkQueue.sql")
                .Replace("public.", $"{options.Schema}.");
            var destructiveDequeue =
                EmbeddedSqlReader.GetEmbeddedSql("TownSuite.WorkQueues.Postgres.sql.public.WorkQueue_Dequeue.sql")
                    .Replace("public.", $"{options.Schema}.");
            var nonDestructiveDequeue =
                EmbeddedSqlReader.GetEmbeddedSql(
                        "TownSuite.WorkQueues.Postgres.sql.public.WorkQueue_Dequeue_NonDestructive.sql")
                    .Replace("public.", $"{options.Schema}.");
            var enqueueWorkeQueue =
                EmbeddedSqlReader.GetEmbeddedSql("TownSuite.WorkQueues.Postgres.sql.public.WorkQueue_Enqueue.sql")
                    .Replace("public.", $"{options.Schema}.");


            using (var connection = new Npgsql.NpgsqlConnection(options.AdminConnectionString))
            {
                await connection.OpenAsync(cancellationToken);
                var command = connection.CreateCommand();
                command.CommandText = $"CREATE SCHEMA IF NOT EXISTS {options.Schema};";
                await command.ExecuteNonQueryAsync(cancellationToken);

                command.CommandText = workeQueueTable;
                await command.ExecuteNonQueryAsync(cancellationToken);
                
                command.CommandText = destructiveDequeue;
                await command.ExecuteNonQueryAsync(cancellationToken);
                
                command.CommandText = nonDestructiveDequeue;
                await command.ExecuteNonQueryAsync(cancellationToken);
                
                command.CommandText = enqueueWorkeQueue;
                await command.ExecuteNonQueryAsync(cancellationToken);

                _logger.LogInformation("Migrations completed successfully.");
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "An error occurred while running migrations.");
            throw;
        }
    }
}