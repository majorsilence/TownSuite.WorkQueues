using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace TownSuite.WorkQueues.Redis;

/// <summary>DI registration helpers for the Redis backend.</summary>
public static class RedisServiceExtensions
{
    /// <summary>
    /// Registers <see cref="IConnectionMultiplexer"/> from a Redis connection string.
    /// Skip this if you are already registering <see cref="IConnectionMultiplexer"/> elsewhere.
    /// </summary>
    public static IServiceCollection AddRedisConnection(
        this IServiceCollection services,
        string connectionString)
    {
        services.AddSingleton<IConnectionMultiplexer>(
            _ => ConnectionMultiplexer.Connect(connectionString));
        return services;
    }

    /// <summary>
    /// Registers <see cref="RedisMessageBus"/> as <see cref="IMessageBus"/> singleton.
    /// Requires <see cref="IConnectionMultiplexer"/> to already be registered.
    /// </summary>
    public static IServiceCollection AddRedisMessageBus(
        this IServiceCollection services,
        Action<RedisOptions> configure)
    {
        var opts = new RedisOptions();
        configure(opts);
        services.AddSingleton(opts);

        services.AddSingleton<IMessageBus>(sp =>
        {
            var redis   = sp.GetRequiredService<IConnectionMultiplexer>();
            var options = sp.GetRequiredService<RedisOptions>();
            var logger  = sp.GetRequiredService<ILogger<RedisMessageBus>>();
            return new RedisMessageBus(redis, options, logger);
        });

        return services;
    }

    /// <summary>
    /// Registers <see cref="RedisWorkQueue"/> as <see cref="IRedisWorkQueue"/> singleton.
    /// Requires <see cref="IConnectionMultiplexer"/> to already be registered.
    /// </summary>
    public static IServiceCollection AddRedisWorkQueue(
        this IServiceCollection services,
        Action<RedisOptions> configure)
    {
        var opts = new RedisOptions();
        configure(opts);
        services.AddSingleton(opts);

        services.AddSingleton<IRedisWorkQueue>(sp =>
        {
            var redis   = sp.GetRequiredService<IConnectionMultiplexer>();
            var options = sp.GetRequiredService<RedisOptions>();
            return new RedisWorkQueue(redis, options);
        });

        return services;
    }
}
