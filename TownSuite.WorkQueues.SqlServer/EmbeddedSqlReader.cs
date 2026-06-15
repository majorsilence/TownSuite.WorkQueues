namespace TownSuite.WorkQueues.SqlServer;

internal static class EmbeddedSqlReader
{
    internal static string GetEmbeddedSql(string resourceName)
    {
        var assembly = typeof(EmbeddedSqlReader).Assembly;
        using var stream = assembly.GetManifestResourceStream(resourceName)
            ?? throw new InvalidOperationException($"Embedded resource '{resourceName}' not found.");
        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }
}
