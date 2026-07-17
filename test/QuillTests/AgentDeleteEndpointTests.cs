using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI.Agents;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

/// <summary>
/// Coverage for <c>DELETE /api/apps/{slug}/agent/{agentId}</c>: removes a
/// provisioned RavenDB AI agent from the per-app DB. Guards: unknown slug/agent
/// return 404; an agent still referenced by a channel / live embed-link is
/// refused with 409 so the embed page never resolves to a missing agent.
/// </summary>
public class AgentDeleteEndpointTests(ITestOutputHelper output) : ApplianceMetricsTestBase(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Delete_removes_agent_and_returns_204()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);
        await SeedAgentAsync(store, perAppDb, name: "Support");
        var agentId = await FirstAgentIdAsync(store, perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.DeleteAsync($"/api/apps/my-app/agent/{agentId}");
        Assert.Equal(HttpStatusCode.NoContent, resp.StatusCode);

        var agents = await store.AI.ForDatabase(perAppDb).GetAgentsAsync();
        Assert.Empty(agents.AiAgents);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Delete_returns_404_for_unknown_slug()
    {
        var store = GetDocumentStore();
        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.DeleteAsync("/api/apps/nonexistent/agent/whatever");
        Assert.Equal(HttpStatusCode.NotFound, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Delete_returns_404_for_unknown_agent()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.DeleteAsync("/api/apps/my-app/agent/does-not-exist");
        Assert.Equal(HttpStatusCode.NotFound, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Delete_returns_409_when_a_channel_is_bound()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);
        await SeedAgentAsync(store, perAppDb, name: "Support");
        var agentId = await FirstAgentIdAsync(store, perAppDb);

        // a channel pointing at the agent blocks the delete
        await SeedChannelAsync(store, perAppDb, channelId: "wgt1", enabled: true, agentId: agentId);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.DeleteAsync($"/api/apps/my-app/agent/{agentId}");
        Assert.Equal(HttpStatusCode.Conflict, resp.StatusCode);

        // agent must still exist — the delete was refused, not partially applied
        var agents = await store.AI.ForDatabase(perAppDb).GetAgentsAsync();
        Assert.Single(agents.AiAgents);
    }

    private static async Task<string> FirstAgentIdAsync(IDocumentStore store, string database)
    {
        var agents = await store.Maintenance.ForDatabase(database).SendAsync(new GetAiAgentsOperation());
        return agents.AiAgents![0].Identifier!;
    }
}
