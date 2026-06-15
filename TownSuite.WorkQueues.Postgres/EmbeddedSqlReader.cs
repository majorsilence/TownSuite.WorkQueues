namespace TownSuite.WorkQueues.Postgres;

internal static class EmbeddedSqlReader
{
    public static string GetEmbeddedSql(string resourceName)
    {
        var assembly = typeof(EmbeddedSqlReader).Assembly;
        using (var stream = assembly.GetManifestResourceStream(resourceName))
        {
            if (stream == null)
            {
                throw new InvalidOperationException($"Resource '{resourceName}' not found.");
            }
            using (var reader = new System.IO.StreamReader(stream))
            {
                return reader.ReadToEnd();
            }
        }
    }
    
}