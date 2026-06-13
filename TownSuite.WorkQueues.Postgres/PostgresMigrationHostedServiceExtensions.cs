using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace TownSuite.WorkQueues.Postgres;

public static class PostgresMigrationHostedServiceExtensions
{
    /// <summary>
    /// Registers <see cref="PostgresMigrationHostedService"/> so that the workqueue
    /// schema, table, stored procedures, and index are created/updated on startup.
    /// Requires <see cref="SqlTransportOptions"/> to be registered.
    /// </summary>
    public static IServiceCollection AddPostgresMigrationHostedService(this IServiceCollection services)
    {
        services.AddHostedService<PostgresMigrationHostedService>();
        return services;
    }

    /// <summary>
    /// Registers <see cref="PostgresMessageBus"/> as the <see cref="IMessageBus"/> singleton.
    /// Call <see cref="IMessageBus.Subscribe{T}"/> on the bus inside the factory delegate
    /// before it is returned.
    /// Requires <see cref="SqlTransportOptions"/> to be registered.
    /// </summary>
    public static IServiceCollection AddPostgresMessageBus(
        this IServiceCollection services,
        Action<IServiceProvider, PostgresMessageBus> configure)
    {
        services.AddSingleton<IMessageBus>(sp =>
        {
            var options = sp.GetRequiredService<SqlTransportOptions>();
            var logger  = sp.GetRequiredService<ILogger<PostgresMessageBus>>();
            var bus     = new PostgresMessageBus(options, logger, sp);
            configure(sp, bus);
            return bus;
        });
        return services;
    }
}
