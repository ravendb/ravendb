using System.Text.Json;
using Raven.Client.Documents.Operations.AI.Agents;

namespace Raven.Quill.Agents;

public static class AgentOutputShape
{
    public const string DefaultReplyField = "reply";

    public static string ResolveReplyField(AiAgentConfiguration? config)
    {
        if (config is null)
            return DefaultReplyField;

        if (TryFirstObjectProperty(config.SampleObject, out var fromSample))
            return fromSample;

        if (TryFirstSchemaProperty(config.OutputSchema, out var fromSchema))
            return fromSchema;

        return DefaultReplyField;
    }

    public static string ExtractReplyText(IReadOnlyDictionary<string, object>? answer, string replyField)
    {
        if (answer is null)
            return "";

        foreach (var (key, value) in answer)
        {
            if (string.Equals(key, replyField, StringComparison.OrdinalIgnoreCase))
                return value?.ToString() ?? "";
        }

        return "";
    }

    private static bool TryFirstObjectProperty(string? json, out string name)
    {
        name = "";
        if (string.IsNullOrWhiteSpace(json))
            return false;

        try
        {
            using var doc = JsonDocument.Parse(json);
            if (doc.RootElement.ValueKind != JsonValueKind.Object)
                return false;

            foreach (var property in doc.RootElement.EnumerateObject())
            {
                name = property.Name;
                return true;
            }
        }
        catch (JsonException)
        {
        }

        return false;
    }

    private static bool TryFirstSchemaProperty(string? json, out string name)
    {
        name = "";
        if (string.IsNullOrWhiteSpace(json))
            return false;

        try
        {
            using var doc = JsonDocument.Parse(json);
            if (doc.RootElement.ValueKind != JsonValueKind.Object ||
                doc.RootElement.TryGetProperty("properties", out var properties) == false ||
                properties.ValueKind != JsonValueKind.Object)
                return false;

            foreach (var property in properties.EnumerateObject())
            {
                name = property.Name;
                return true;
            }
        }
        catch (JsonException)
        {
        }

        return false;
    }
}
