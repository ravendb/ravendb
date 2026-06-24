using System.Net.Http.Json;
using System.Text.Json;
using Raven.AiAppliance.Metrics;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI.Agents;
using Tests.Infrastructure;
using Xunit;

namespace AiApplianceTests;

/// <summary>
/// The agents list (<c>GET /api/apps/{slug}/agents</c>) is enriched with usage from
/// the conversation index — <c>invocations</c> (conversation count) and
/// <c>lastInvokedAt</c> — joined to the provisioned agent by its identifier.
/// </summary>
public class AgentsListEndpointTests(ITestOutputHelper output) : ApplianceMetricsTestBase(output)
{
    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Agents_list_includes_invocations_and_last_invoked()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);
        await new ConversationMetricsIndex().ExecuteAsync(store, database: perAppDb);
        await SeedAgentAsync(store, perAppDb, name: "Support");

        var agents = await store.Maintenance.ForDatabase(perAppDb).SendAsync(new GetAiAgentsOperation());
        var agentId = agents.AiAgents![0].Identifier;

        var now = DateTime.UtcNow;
        await SeedConversationAsync(store, perAppDb, "chats/a", agentId, now.AddHours(-1));
        await SeedConversationAsync(store, perAppDb, "chats/b", agentId, now.AddHours(-2));
        await Indexes.WaitForIndexingAsync(store, perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var list = await client.GetFromJsonAsync<JsonElement>("/api/apps/my-app/agents");
        var agent = list.EnumerateArray().Single(a => a.GetProperty("agentId").GetString() == agentId);

        Assert.Equal(2, agent.GetProperty("invocations").GetInt64());          // two conversations
        Assert.Equal("gpt-4o-mini", agent.GetProperty("model").GetString());   // from the connection string
        Assert.Equal(JsonValueKind.String, agent.GetProperty("lastInvokedAt").ValueKind); // present
        Assert.EndsWith("Z\"", agent.GetProperty("lastInvokedAt").GetRawText()); // I1: UTC, ISO-8601 with Z
    }
}
