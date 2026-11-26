using System;
using System.Collections.Generic;
using System.Text.Json.Serialization.Metadata;

namespace Raven.Client.Json.Serialization.SystemTextJson.Internal;

internal sealed class JsonPropertyMap
{
    public readonly Dictionary<string, JsonPropertyInfo> ByName;

    public readonly Dictionary<string, JsonPropertyInfo> Shadowed;

    public JsonPropertyMap(JsonTypeInfo typeInfo, bool ignoreCase)
    {
        ByName = new Dictionary<string, JsonPropertyInfo>(ignoreCase ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal);
        if (typeInfo.Kind != JsonTypeInfoKind.Object)
            return;

        foreach (JsonPropertyInfo property in typeInfo.Properties)
        {
            if (property.AttributeProvider == null && property.Get == null)
                continue;

            if (ByName.ContainsKey(property.Name))
                (Shadowed ??= new Dictionary<string, JsonPropertyInfo>(StringComparer.Ordinal))[property.Name] = property;
            else
                ByName.Add(property.Name, property);
        }
    }

    public int Count => ByName.Count + (Shadowed?.Count ?? 0);

    public bool TryGetValue(string name, out JsonPropertyInfo property)
    {
        if (Shadowed != null && Shadowed.TryGetValue(name, out property))
            return true;

        return ByName.TryGetValue(name, out property);
    }
}
