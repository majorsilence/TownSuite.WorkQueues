using DotNet.Testcontainers.Containers;
using Microsoft.Data.SqlClient;
using Npgsql;
using System.ComponentModel;
using System.Data.Common;
using System.Text;
using Testcontainers.MsSql;
using Testcontainers.PostgreSql;

namespace TownSuite.WorkQueues.Testing;

sealed class TestContainerWrapper : IAsyncDisposable
{
    public readonly IDatabaseContainer Container;
    public Func<DbConnection> CreateConnection { get; }

    public TestContainerWrapper(IDatabaseContainer container, Func<DbConnection> connectionFactory)
    {
        Container = container;
        CreateConnection = connectionFactory;
    }

    public async Task StartAsync()
    {
        // both MsSqlContainer and PostgreSqlContainer expose StartAsync()
        await Container.StartAsync();
        if (Container is MsSqlContainer)
        {
            await BringUpDatabaseSqlServer(Container.GetConnectionString());
        }
        else if (Container is PostgreSqlContainer)
        {
            await BringUpDatabasePostgresql(Container.GetConnectionString());
        }
        else
        {
            throw new InvalidOperationException("Unsupported container type");
        }
    }

    public async ValueTask DisposeAsync()
    {
        // both expose DisposeAsync()
        await Container.DisposeAsync();
    }

    public static async Task<TestContainerWrapper> CreateContainerAsync(string backend)
    {
        if (backend == "mssql")
        {
            var container = new MsSqlBuilder()
                .Build();

            var wrapper = new TestContainerWrapper(
                container,
                () => (DbConnection)new SqlConnection(container.GetConnectionString())
            );
            return wrapper;
        }
        else if (backend == "postgres")
        {
            var container = new PostgreSqlBuilder()
                .Build();

            var wrapper = new TestContainerWrapper(
                container,
                () => (DbConnection)new NpgsqlConnection(container.GetConnectionString())
            );
            return wrapper;
        }

        throw new ArgumentException("Unsupported backend", nameof(backend));
    }

    private static async Task BringUpDatabaseSqlServer(string connectionString)
    {
        // run the scripts from the sql-server foler to create the database and the tables via SMO
        StringBuilder script = new StringBuilder();
        var files = Directory.GetFiles("sql-server", "*.sql").OrderBy(f => f);
        foreach (var file in files)
        {
            script.AppendLine(File.ReadAllText(file));
        }

        using var sqlConnection = new SqlConnection(connectionString);
        var serverConnection = new Microsoft.SqlServer.Management.Common.ServerConnection(sqlConnection);
        serverConnection.Connect();
        serverConnection.ExecuteNonQuery(script.ToString());
        serverConnection.Disconnect();
    }

    private static async Task BringUpDatabasePostgresql(string connectionString)
    {
        // run the scripts from the postgresql foler to create the database and the tables via npgsql
        StringBuilder script = new StringBuilder();
        var files = Directory.GetFiles("postgresql", "*.sql").OrderBy(f => f);
        foreach (var file in files)
        {
            script.AppendLine(File.ReadAllText(file));
        }

        using var npgsqlConnection = new NpgsqlConnection(connectionString);
        var command = new NpgsqlCommand(script.ToString(), npgsqlConnection);
        command.CommandType = System.Data.CommandType.Text;
        command.CommandTimeout = 60; // increase timeout for long-running scripts
        command.CommandText = script.ToString();
        await npgsqlConnection.OpenAsync();
        await command.ExecuteNonQueryAsync();
    }
}