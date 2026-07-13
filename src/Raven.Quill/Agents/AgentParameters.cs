using Raven.Client.Documents.Operations.AI.Agents;

namespace Raven.Quill.Agents;

/// <summary>
/// Resolves an agent's declared chat-scoped parameters against a supplied set.
/// Shared by the embed-link mint endpoint (validates + binds parameters at mint
/// time) — the public chat surface no longer accepts parameters at all
/// (RavenDB-26775). Only declared names pass through (undeclared dropped); a
/// missing or blank declared value fails. Supplied keys match case-insensitively;
/// the agent's declared casing is what is kept.
/// </summary>
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

        // Indexer (not the copying ctor) so supplied keys differing only by case
        // can't throw — last one wins.
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
