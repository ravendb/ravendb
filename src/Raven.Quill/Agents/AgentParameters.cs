using System.Text.Json;
using Raven.Client.Documents.Operations.AI.Agents;

namespace Raven.Quill.Agents;

public sealed record AgentParameterResolution(
    Dictionary<string, string> Resolved,
    List<string> Missing,
    List<string> Invalid)
{
    public bool IsValid => Missing.Count == 0 && Invalid.Count == 0;
}

public static class AgentParameters
{
    public static AgentParameterResolution Resolve(
        AiAgentConfiguration config, IReadOnlyDictionary<string, JsonElement>? supplied)
    {
        var resolution = new AgentParameterResolution(new Dictionary<string, string>(), [], []);

        var declared = (config.Parameters ?? [])
            .Where(parameter => string.IsNullOrWhiteSpace(parameter.Name) == false)
            .ToArray();
        if (declared.Length == 0)
            return resolution;

        var suppliedByName = new Dictionary<string, JsonElement>(StringComparer.OrdinalIgnoreCase);
        foreach (var (key, value) in supplied ?? new Dictionary<string, JsonElement>())
            suppliedByName[key] = value;

        foreach (var parameter in declared)
        {
            var present = suppliedByName.TryGetValue(parameter.Name, out var value) &&
                          AgentParameterValue.IsBlank(value) == false;

            if (present == false)
            {
                if (parameter.Type == AiAgentParameterValueType.Null)
                    resolution.Resolved[parameter.Name] = AgentParameterValue.ToStoredText(NullElement);
                else
                    resolution.Missing.Add(parameter.Name);

                continue;
            }

            if (AgentParameterValue.TryNormalize(parameter.Type, value, out var normalized, out var error) == false)
            {
                resolution.Invalid.Add($"{parameter.Name}: {error}");
                continue;
            }

            resolution.Resolved[parameter.Name] = AgentParameterValue.ToStoredText(normalized);
        }

        return resolution;
    }

    public static string Describe(AgentParameterResolution resolution)
    {
        var parts = new List<string>();

        if (resolution.Missing.Count > 0)
            parts.Add($"missing agent parameter(s): {string.Join(", ", resolution.Missing)}");

        if (resolution.Invalid.Count > 0)
            parts.Add($"invalid agent parameter(s): {string.Join("; ", resolution.Invalid)}");

        return string.Join(". ", parts);
    }

    private static readonly JsonElement NullElement = JsonSerializer.SerializeToElement<object?>(null);
}
