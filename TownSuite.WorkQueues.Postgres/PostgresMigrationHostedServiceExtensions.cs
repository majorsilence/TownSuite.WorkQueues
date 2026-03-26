using Microsoft.Extensions.DependencyInjection;

namespace TownSuite.WorkQueues.Postgres;

public static class PostgresMigrationHostedServiceExtensions
{
    public static IServiceCollection AddPostgresMigrationHostedService(this IServiceCollection services)
    {
        // Register the hosted service that will run migrations on startup.
        services.AddHostedService<PostgresMigrationHostedService>();
        return services;
    }
}
