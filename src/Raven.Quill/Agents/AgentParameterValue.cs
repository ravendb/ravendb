using System.Globalization;
using System.Text.Json;
using Raven.Client.Documents.Operations.AI.Agents;

namespace Raven.Quill.Agents;

public static class AgentParameterValue
{
    public static JsonElement FromStoredText(string? text)
    {
        if (string.IsNullOrEmpty(text))
            return FromString("");

        try
        {
            using var document = JsonDocument.Parse(text);
            return document.RootElement.Clone();
        }
        catch (JsonException)
        {
            return FromString(text);
        }
    }

    public static JsonElement FromString(string? value) => JsonSerializer.SerializeToElement(value ?? "");

    public static string ToStoredText(JsonElement value) => value.GetRawText();

    public static string ToDisplayText(JsonElement value) =>
        value.ValueKind == JsonValueKind.String ? value.GetString() ?? "" : value.GetRawText();

    public static bool IsBlank(JsonElement value) =>
        value.ValueKind switch
        {
            JsonValueKind.Undefined => true,
            JsonValueKind.Null => true,
            JsonValueKind.String => string.IsNullOrWhiteSpace(value.GetString()),
            _ => false,
        };

    public static bool TryNormalize(
        AiAgentParameterValueType type, JsonElement supplied, out JsonElement normalized, out string? error)
    {
        if (type == AiAgentParameterValueType.Default)
        {
            normalized = supplied;
            error = null;
            return true;
        }

        if (TryConvert(type, supplied, out var value, out error) == false)
        {
            normalized = default;
            return false;
        }

        normalized = JsonSerializer.SerializeToElement(value);
        return true;
    }

    private static bool TryConvert(
        AiAgentParameterValueType type, JsonElement supplied, out object? value, out string? error)
    {
        value = null;
        error = null;

        switch (type)
        {
            case AiAgentParameterValueType.String:
                return TryConvertString(supplied, out value, out error);
            case AiAgentParameterValueType.Number:
                return TryConvertNumber(supplied, out value, out error);
            case AiAgentParameterValueType.Boolean:
                return TryConvertBoolean(supplied, out value, out error);
            case AiAgentParameterValueType.ArrayOfString:
            case AiAgentParameterValueType.ArrayOfNumber:
            case AiAgentParameterValueType.ArrayOfBoolean:
                return TryConvertArray(type, supplied, out value, out error);
            case AiAgentParameterValueType.Null:
                return TryConvertNull(supplied, out value, out error);
            default:
                error = $"unsupported parameter type '{type}'";
                return false;
        }
    }

    private static bool TryConvertString(JsonElement supplied, out object? value, out string? error)
    {
        value = null;
        error = null;

        switch (supplied.ValueKind)
        {
            case JsonValueKind.String:
                value = supplied.GetString();
                return true;
            case JsonValueKind.Number:
            case JsonValueKind.True:
            case JsonValueKind.False:
                value = supplied.GetRawText();
                return true;
            default:
                error = $"expected a string, got {Describe(supplied)}";
                return false;
        }
    }

    private static bool TryConvertNumber(JsonElement supplied, out object? value, out string? error)
    {
        value = null;
        error = null;

        if (supplied.ValueKind == JsonValueKind.Number)
        {
            if (supplied.TryGetInt64(out var integer))
            {
                value = integer;
                return true;
            }

            if (supplied.TryGetDouble(out var approximate) && double.IsFinite(approximate))
            {
                value = supplied.TryGetDecimal(out var precise) && (double)precise == approximate
                    ? precise
                    : approximate;
                return true;
            }

            error = $"expected a number a double can hold, got {Describe(supplied)}";
            return false;
        }

        if (supplied.ValueKind == JsonValueKind.String && TryParseNumber(supplied.GetString(), out value))
            return true;

        error = $"expected a number, got {Describe(supplied)}";
        return false;
    }

    private static bool TryParseNumber(string? text, out object? value)
    {
        value = null;

        var trimmed = text?.Trim();
        if (string.IsNullOrEmpty(trimmed) || trimmed[0] == '+')
            return false;

        if (long.TryParse(trimmed, NumberStyles.Integer, CultureInfo.InvariantCulture, out var integer))
        {
            value = integer;
            return true;
        }

        if (double.TryParse(trimmed, NumberStyles.Float, CultureInfo.InvariantCulture, out var approximate) == false ||
            double.IsFinite(approximate) == false)
            return false;

        value = decimal.TryParse(trimmed, NumberStyles.Float, CultureInfo.InvariantCulture, out var precise) &&
                (double)precise == approximate
            ? precise
            : approximate;

        return true;
    }

    private static bool TryConvertBoolean(JsonElement supplied, out object? value, out string? error)
    {
        value = null;
        error = null;

        switch (supplied.ValueKind)
        {
            case JsonValueKind.True:
            case JsonValueKind.False:
                value = supplied.GetBoolean();
                return true;
            case JsonValueKind.String when bool.TryParse(supplied.GetString()?.Trim(), out var parsed):
                value = parsed;
                return true;
            default:
                error = $"expected a boolean, got {Describe(supplied)}";
                return false;
        }
    }

    private static bool TryConvertArray(
        AiAgentParameterValueType type, JsonElement supplied, out object? value, out string? error)
    {
        value = null;

        var elementType = ElementTypeOf(type);

        if (supplied.ValueKind == JsonValueKind.String)
        {
            if (TrySplitArrayText(supplied.GetString(), elementType, out value, out error) == false)
                return false;

            return true;
        }

        if (supplied.ValueKind != JsonValueKind.Array)
        {
            error = $"expected an array, got {Describe(supplied)}";
            return false;
        }

        var items = new List<object?>();
        foreach (var element in supplied.EnumerateArray())
        {
            if (TryConvert(elementType, element, out var item, out error) == false)
                return false;

            items.Add(item);
        }

        value = items;
        error = null;
        return true;
    }

    private static bool TrySplitArrayText(
        string? text, AiAgentParameterValueType elementType, out object? value, out string? error)
    {
        value = null;
        error = null;

        var trimmed = text?.Trim() ?? "";
        if (trimmed.Length == 0)
        {
            value = new List<object?>();
            return true;
        }

        if (trimmed.StartsWith('['))
        {
            JsonElement parsed;
            try
            {
                using var document = JsonDocument.Parse(trimmed);
                parsed = document.RootElement.Clone();
            }
            catch (JsonException)
            {
                error = $"expected a JSON array, got '{trimmed}'";
                return false;
            }

            return TryConvertArray(ArrayTypeOf(elementType), parsed, out value, out error);
        }

        var items = new List<object?>();
        foreach (var part in trimmed.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries))
        {
            if (TryConvert(elementType, FromString(part), out var item, out error) == false)
                return false;

            items.Add(item);
        }

        value = items;
        return true;
    }

    private static bool TryConvertNull(JsonElement supplied, out object? value, out string? error)
    {
        value = null;

        if (IsBlank(supplied))
        {
            error = null;
            return true;
        }

        error = $"expected null, got {Describe(supplied)}";
        return false;
    }

    private static AiAgentParameterValueType ElementTypeOf(AiAgentParameterValueType type) =>
        type switch
        {
            AiAgentParameterValueType.ArrayOfString => AiAgentParameterValueType.String,
            AiAgentParameterValueType.ArrayOfNumber => AiAgentParameterValueType.Number,
            AiAgentParameterValueType.ArrayOfBoolean => AiAgentParameterValueType.Boolean,
            _ => AiAgentParameterValueType.Default,
        };

    private static AiAgentParameterValueType ArrayTypeOf(AiAgentParameterValueType elementType) =>
        elementType switch
        {
            AiAgentParameterValueType.String => AiAgentParameterValueType.ArrayOfString,
            AiAgentParameterValueType.Number => AiAgentParameterValueType.ArrayOfNumber,
            AiAgentParameterValueType.Boolean => AiAgentParameterValueType.ArrayOfBoolean,
            _ => AiAgentParameterValueType.Default,
        };

    private static string Describe(JsonElement value) =>
        value.ValueKind switch
        {
            JsonValueKind.String => $"'{value.GetString()}'",
            JsonValueKind.Array => "an array",
            JsonValueKind.Object => "an object",
            JsonValueKind.Null or JsonValueKind.Undefined => "null",
            _ => value.GetRawText(),
        };
}
