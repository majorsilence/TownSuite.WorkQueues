using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace TownSuite.WorkQueues.SqlServer;

/// <summary>DI registration helpers for the SQL Server backend.</summary>
public static class SqlServerServiceExtensions
{
    /// <summary>
    /// Registers <see cref="SqlServerMigrationHostedService"/> so that the workqueue
    /// table, stored procedures, and index are created/updated on application startup.
    /// Requires <see cref="SqlServerTransportOptions"/> to be registered.
    /// </summary>
    public static IServiceCollection AddSqlServerMigrationHostedService(
        this IServiceCollection services)
    {
        services.AddHostedService<SqlServerMigrationHostedService>();
        return services;
    }

    /// <summary>
    /// Registers <see cref="SqlServerMessageBus"/> as the <see cref="IMessageBus"/> singleton.
    /// Call <see cref="IMessageBus.Subscribe{T}"/> on the returned bus inside the factory
    /// delegate before it is returned.
    /// Requires <see cref="SqlServerTransportOptions"/> to be registered.
    /// </summary>
    public static IServiceCollection AddSqlServerMessageBus(
        this IServiceCollection services,
        Action<IServiceProvider, SqlServerMessageBus> configure)
    {
        services.AddSingleton<IMessageBus>(sp =>
        {
            var options = sp.GetRequiredService<SqlServerTransportOptions>();
            var logger  = sp.GetRequiredService<ILogger<SqlServerMessageBus>>();
            var bus     = new SqlServerMessageBus(options, logger);
            configure(sp, bus);
            return bus;
        });
        return services;
    }
}
