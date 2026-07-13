using System.Text.Json;
using Raven.Client.Documents.Operations.AI.Agents;

namespace Raven.Quill.Agents;

/// <summary>
/// Derives, at runtime, an agent's reply field — the single output property whose
/// value the client streams chunk-by-chunk — from the persisted
/// <see cref="AiAgentConfiguration"/> rather than a compile-time answer type.
/// Resolution mirrors how the output shape is declared: the first property of
/// <see cref="AiAgentConfiguration.SampleObject"/> (the JSON the registrar seeds
/// and the wizard fills), falling back to the first key under the
/// <see cref="AiAgentConfiguration.OutputSchema"/>'s <c>properties</c>, then to
/// <c>"reply"</c>. The client streams the first declared property by convention,
/// so deriving the path from the same sample that steers the model keeps the
/// streamed path and the model's output in lockstep.
/// </summary>
public static class AgentOutputShape
{
    public const string DefaultReplyField = "reply";

    public static string ResolveReplyField(AiAgentConfiguration config)
    {
        if (TryFirstObjectProperty(config.SampleObject, out var fromSample))
            return fromSample;

        if (TryFirstSchemaProperty(config.OutputSchema, out var fromSchema))
            return fromSchema;

        return DefaultReplyField;
    }

    /// <summary>Reads the reply text out of a data-driven answer object,
    /// matching <paramref name="replyField"/> case-insensitively (the dictionary
    /// produced by the RavenDB client uses an ordinal key comparer). Returns the
    /// empty string when the field is absent.</summary>
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
            // Malformed sample — fall through to the next resolution source.
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
            // Malformed schema — fall through to the default.
        }

        return false;
    }
}
