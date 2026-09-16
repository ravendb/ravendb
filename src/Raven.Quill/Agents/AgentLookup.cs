using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI.Agents;

namespace Raven.Quill.Agents;

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
