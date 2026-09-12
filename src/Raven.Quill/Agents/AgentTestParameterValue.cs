using System.Text.Json;
using Sparrow.Json.Parsing;

namespace Raven.Quill.Agents;

public static class AgentTestParameterValue
{
    public static object? Convert(JsonElement? value) => value is null ? null : ToRavenJsonValue(value.Value);

    private static object? ToRavenJsonValue(JsonElement element)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Null:
            case JsonValueKind.Undefined:
                return null;
            case JsonValueKind.String:
                return element.GetString();
            case JsonValueKind.True:
            case JsonValueKind.False:
                return element.GetBoolean();
            case JsonValueKind.Number:
                if (element.TryGetInt64(out var integer))
                    return integer;
                if (element.TryGetDecimal(out var decimalValue))
                    return decimalValue;
                return element.GetDouble();
            case JsonValueKind.Array:
                var array = new DynamicJsonArray();
                foreach (var item in element.EnumerateArray())
                    array.Add(ToRavenJsonValue(item));
                return array;
            case JsonValueKind.Object:
                var result = new DynamicJsonValue();
                foreach (var property in element.EnumerateObject())
                    result[property.Name] = ToRavenJsonValue(property.Value);
                return result;
            default:
                throw new ArgumentOutOfRangeException(nameof(element), element.ValueKind, "Unsupported JSON value kind.");
        }
    }
}
