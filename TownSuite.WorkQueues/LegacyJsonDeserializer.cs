using System.Runtime.CompilerServices;
using System.Text.Json;

[assembly: InternalsVisibleTo("TownSuite.WorkQueues.Postgres")]
[assembly: InternalsVisibleTo("TownSuite.WorkQueues.Redis")]
[assembly: InternalsVisibleTo("TownSuite.WorkQueues.Testing")]

namespace TownSuite.WorkQueues;

/// <summary>
/// Deserializes JSON payloads written by either the current System.Text.Json serializer
/// or the legacy Newtonsoft.Json TypeNameHandling.All format used in v1.x.
///
/// For most payloads (POCOs, nested objects) System.Text.Json handles the old format
/// automatically by ignoring the "$type" property. The one exception is collection
/// root types: Newtonsoft serialised them as {"$type":"...","$values":[...]} rather
/// than a plain JSON array. This helper detects that wrapper and extracts the array
/// before deserialising so existing queued messages are not lost on upgrade.
/// </summary>
internal static class LegacyJsonDeserializer
{
    internal static T? Deserialize<T>(string json)
    {
        try
        {
            return JsonSerializer.Deserialize<T>(json);
        }
        catch (JsonException) when (json.Contains("\"$values\""))
        {
            return (T?)ExtractValues(json, typeof(T));
        }
    }

    internal static object? Deserialize(string json, Type type)
    {
        try
        {
            return JsonSerializer.Deserialize(json, type);
        }
        catch (JsonException) when (json.Contains("\"$values\""))
        {
            return ExtractValues(json, type);
        }
    }

    private static object? ExtractValues(string json, Type type)
    {
        using var doc = JsonDocument.Parse(json);
        if (doc.RootElement.TryGetProperty("$values", out var values))
            return JsonSerializer.Deserialize(values.GetRawText(), type);
        return null;
    }
}
