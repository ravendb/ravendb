using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI.Agents;

namespace Raven.AiAppliance.Agents;

/// <summary>
/// Resolves a provisioned agent in a database by identifier. Lists the database's
/// agents — a non-throwing read; the single-id <c>GetAgentAsync</c> throws when the
/// id is missing — and matches case-insensitively, mirroring the case-insensitive
/// resolution the removed compile-time registry provided. Returns <c>null</c> when
/// nothing matches; callers turn that into a clean 4xx.
/// </summary>
internal static class AgentLookup
{
    public static async Task<AiAgentConfiguration?> FindAsync(
        IDocumentStore store, string database, string agentId, CancellationToken ct)
    {
        var all = await store.AI.ForDatabase(database).GetAgentsAsync(ct);
        return all.AiAgents?.FirstOrDefault(a =>
            string.Equals(a.Identifier, agentId, StringComparison.OrdinalIgnoreCase));
    }
}
