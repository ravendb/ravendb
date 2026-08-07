using Raven.Client.Documents.Operations.AI.Agents;

namespace Raven.Quill.Agents;

public static class AgentParameters
{
    public static bool TryResolve(
        AiAgentConfiguration config,
        IReadOnlyDictionary<string, string>? supplied,
        out Dictionary<string, string> resolved,
        out List<string> missing)
    {
        resolved = new Dictionary<string, string>();
        missing = [];

        var declared = (config.Parameters ?? [])
            .Select(parameter => parameter.Name)
            .Where(name => string.IsNullOrWhiteSpace(name) == false)
            .ToArray();
        if (declared.Length == 0)
            return true;

        var suppliedByName = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var (key, value) in supplied ?? new Dictionary<string, string>())
            suppliedByName[key] = value;

        foreach (var name in declared)
        {
            if (suppliedByName.TryGetValue(name, out var value) && string.IsNullOrWhiteSpace(value) == false)
                resolved[name] = value;
            else
                missing.Add(name);
        }

        return missing.Count == 0;
    }
}
