using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace TownSuite.WorkQueues.Sqlite;

/// <summary>DI registration helpers for the SQLite backend.</summary>
public static class SqliteServiceExtensions
{
    /// <summary>
    /// Registers <see cref="SqliteMigrationHostedService"/> so that the workqueue table
    /// and index are created on application startup. Requires <see cref="SqliteTransportOptions"/>
    /// to be registered.
    /// </summary>
    public static IServiceCollection AddSqliteMigrationHostedService(
        this IServiceCollection services)
    {
        services.AddHostedService<SqliteMigrationHostedService>();
        return services;
    }

    /// <summary>
    /// Registers <see cref="SqliteMessageBus"/> as the <see cref="IMessageBus"/> singleton.
    /// Call <see cref="IMessageBus.Subscribe{T}"/> on the returned bus inside the factory
    /// delegate before it is returned.
    /// Requires <see cref="SqliteTransportOptions"/> to be registered.
    /// </summary>
    public static IServiceCollection AddSqliteMessageBus(
        this IServiceCollection services,
        Action<IServiceProvider, SqliteMessageBus> configure)
    {
        services.AddSingleton<IMessageBus>(sp =>
        {
            var options = sp.GetRequiredService<SqliteTransportOptions>();
            var logger  = sp.GetRequiredService<ILogger<SqliteMessageBus>>();
            var bus     = new SqliteMessageBus(options, logger, sp);
            configure(sp, bus);
            return bus;
        });
        return services;
    }
}
