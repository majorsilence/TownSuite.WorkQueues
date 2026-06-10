using NUnit.Framework;
using TownSuite.WorkQueues;

namespace TownSuite.WorkQueues.Testing;

/// <summary>
/// Verifies that payloads written by the legacy Newtonsoft.Json TypeNameHandling.All
/// serializer can be deserialized by the current System.Text.Json-based stack.
/// These tests do not require Docker — they are pure unit tests.
/// </summary>
[TestFixture]
public class BackwardsCompatTests
{
    // ── Simple POCO ──────────────────────────────────────────────────────────

    [Test]
    public void SimplePoco_WithTypeAnnotation_Deserializes()
    {
        // Newtonsoft TypeNameHandling.All output for a flat DTO
        var json = """{"$type":"TownSuite.WorkQueues.Testing.LegacyOrder, TownSuite.WorkQueues.Testing","Id":42,"Name":"Widget"}""";

        var result = LegacyJsonDeserializer.Deserialize<LegacyOrder>(json);

        Assert.That(result, Is.Not.Null);
        Assert.That(result!.Id, Is.EqualTo(42));
        Assert.That(result.Name, Is.EqualTo("Widget"));
    }

    // ── Nested POCO ──────────────────────────────────────────────────────────

    [Test]
    public void NestedPoco_WithTypeAnnotations_Deserializes()
    {
        // Every nested object also carries $type with TypeNameHandling.All
        var json = """{"$type":"TownSuite.WorkQueues.Testing.LegacyOrder, TownSuite.WorkQueues.Testing","Id":7,"Name":"Outer","Address":{"$type":"TownSuite.WorkQueues.Testing.LegacyAddress, TownSuite.WorkQueues.Testing","City":"Springfield","Zip":"12345"}}""";

        var result = LegacyJsonDeserializer.Deserialize<LegacyOrder>(json);

        Assert.That(result!.Id, Is.EqualTo(7));
        Assert.That(result.Address!.City, Is.EqualTo("Springfield"));
        Assert.That(result.Address.Zip, Is.EqualTo("12345"));
    }

    // ── Collection root (the one case that requires the $values fallback) ────

    [Test]
    public void ListRoot_WithValuesWrapper_Deserializes()
    {
        // Newtonsoft wraps collection roots as {"$type":"...","$values":[...]}
        // instead of a plain JSON array. System.Text.Json cannot parse this
        // without the $values extraction fallback in LegacyJsonDeserializer.
        var json = """{"$type":"System.Collections.Generic.List`1[[TownSuite.WorkQueues.Testing.LegacyOrder, TownSuite.WorkQueues.Testing]], mscorlib","$values":[{"$type":"TownSuite.WorkQueues.Testing.LegacyOrder, TownSuite.WorkQueues.Testing","Id":1,"Name":"First"},{"$type":"TownSuite.WorkQueues.Testing.LegacyOrder, TownSuite.WorkQueues.Testing","Id":2,"Name":"Second"}]}""";

        var result = LegacyJsonDeserializer.Deserialize<List<LegacyOrder>>(json);

        Assert.That(result, Is.Not.Null);
        Assert.That(result!.Count, Is.EqualTo(2));
        Assert.That(result[0].Id, Is.EqualTo(1));
        Assert.That(result[1].Name, Is.EqualTo("Second"));
    }

    [Test]
    public void ArrayRoot_WithValuesWrapper_Deserializes()
    {
        var json = """{"$type":"TownSuite.WorkQueues.Testing.LegacyOrder[], TownSuite.WorkQueues.Testing","$values":[{"$type":"TownSuite.WorkQueues.Testing.LegacyOrder, TownSuite.WorkQueues.Testing","Id":10,"Name":"A"}]}""";

        var result = LegacyJsonDeserializer.Deserialize<LegacyOrder[]>(json);

        Assert.That(result, Is.Not.Null);
        Assert.That(result!.Length, Is.EqualTo(1));
        Assert.That(result[0].Id, Is.EqualTo(10));
    }

    // ── Modern format still works ─────────────────────────────────────────────

    [Test]
    public void ModernFormat_WithoutTypeAnnotation_Deserializes()
    {
        var json = """{"Id":99,"Name":"Modern"}""";

        var result = LegacyJsonDeserializer.Deserialize<LegacyOrder>(json);

        Assert.That(result!.Id, Is.EqualTo(99));
        Assert.That(result.Name, Is.EqualTo("Modern"));
    }

    [Test]
    public void ModernListFormat_PlainArray_Deserializes()
    {
        var json = """[{"Id":1,"Name":"A"},{"Id":2,"Name":"B"}]""";

        var result = LegacyJsonDeserializer.Deserialize<List<LegacyOrder>>(json);

        Assert.That(result!.Count, Is.EqualTo(2));
    }
}

// Test DTOs — kept here so the type names in the legacy JSON strings above match.
public class LegacyOrder
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public LegacyAddress? Address { get; set; }
}

public class LegacyAddress
{
    public string City { get; set; } = string.Empty;
    public string Zip { get; set; } = string.Empty;
}
