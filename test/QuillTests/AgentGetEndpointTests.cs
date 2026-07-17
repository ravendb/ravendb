using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI.Agents;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

/// <summary>
/// Coverage for <c>GET /api/apps/{slug}/agent/{agentId}</c>: returns the full AI
/// agent configuration (not the projected list summary) so the UI can populate an
/// edit form and POST it back. Unknown slug / agent both return 404.
/// </summary>
public class AgentGetEndpointTests(ITestOutputHelper output) : ApplianceMetricsTestBase(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Get_returns_full_agent_configuration()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);
        await SeedAgentAsync(store, perAppDb, name: "Support");
        var agentId = await FirstAgentIdAsync(store, perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.GetAsync($"/api/apps/my-app/agent/{agentId}");
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        var json = await resp.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal(agentId, json.GetProperty("identifier").GetString());
        Assert.Equal("Support", json.GetProperty("name").GetString());
        Assert.Equal("demo-llm", json.GetProperty("connectionStringName").GetString());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Get_returns_404_for_unknown_slug()
    {
        var store = GetDocumentStore();
        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.GetAsync("/api/apps/nonexistent/agent/whatever");
        Assert.Equal(HttpStatusCode.NotFound, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Get_returns_404_for_unknown_agent()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.GetAsync("/api/apps/my-app/agent/does-not-exist");
        Assert.Equal(HttpStatusCode.NotFound, resp.StatusCode);
    }

    private static async Task<string> FirstAgentIdAsync(IDocumentStore store, string database)
    {
        var agents = await store.Maintenance.ForDatabase(database).SendAsync(new GetAiAgentsOperation());
        return agents.AiAgents![0].Identifier!;
    }
}
