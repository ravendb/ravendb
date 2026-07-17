using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI.Agents;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

/// <summary>
/// Coverage for <c>POST /api/apps/{slug}/agent</c> (edit). Edit is update-only:
/// the body's identifier must point at an existing agent (404 otherwise). It
/// reuses the same demo gating as provisioning (required fields, connection-string /
/// provider checks), so a well-formed update rewrites the stored config in place.
/// </summary>
public class AgentEditEndpointTests(ITestOutputHelper output) : ApplianceMetricsTestBase(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Edit_updates_existing_agent_in_place()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);
        await SeedAgentAsync(store, perAppDb, name: "Support");
        var agentId = await FirstAgentIdAsync(store, perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        // keep the name (identifier is bound to it); change the system prompt
        var resp = await client.PostAsJsonAsync(
            "/api/apps/my-app/agent",
            new
            {
                identifier = agentId,
                name = "Support",
                systemPrompt = "You are the updated support agent.",
                connectionStringName = "demo-llm",
            });

        Assert.True(resp.IsSuccessStatusCode,
            $"edit returned {resp.StatusCode}: {await resp.Content.ReadAsStringAsync()}");
        var json = await resp.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal(agentId, json.GetProperty("agentId").GetString());

        // update in place: still one agent, same id, new prompt applied
        var agents = await store.Maintenance.ForDatabase(perAppDb).SendAsync(new GetAiAgentsOperation());
        var agent = Assert.Single(agents.AiAgents);
        Assert.Equal(agentId, agent.Identifier);
        Assert.Equal("Support", agent.Name);
        Assert.Equal("You are the updated support agent.", agent.SystemPrompt);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Edit_returns_400_when_renaming()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);
        await SeedAgentAsync(store, perAppDb, name: "Support");
        var agentId = await FirstAgentIdAsync(store, perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        // the server binds identifier to name; renaming is refused with a clear 400
        var resp = await client.PostAsJsonAsync(
            "/api/apps/my-app/agent",
            new
            {
                identifier = agentId,
                name = "Renamed",
                systemPrompt = "You help.",
                connectionStringName = "demo-llm",
            });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Edit_returns_404_for_unknown_agent()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);
        await SeedAgentAsync(store, perAppDb, name: "Support");

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.PostAsJsonAsync(
            "/api/apps/my-app/agent",
            new
            {
                identifier = "ghost-agent",
                name = "Renamed",
                systemPrompt = "You help.",
                connectionStringName = "demo-llm",
            });

        Assert.Equal(HttpStatusCode.NotFound, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Edit_returns_404_for_unknown_slug()
    {
        var store = GetDocumentStore();
        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.PostAsJsonAsync(
            "/api/apps/nonexistent/agent",
            new { identifier = "x", name = "Y", systemPrompt = "Z", connectionStringName = "demo-llm" });

        Assert.Equal(HttpStatusCode.NotFound, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Edit_returns_400_when_identifier_missing()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.PostAsJsonAsync(
            "/api/apps/my-app/agent",
            new { name = "Renamed", systemPrompt = "You help.", connectionStringName = "demo-llm" });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Edit_returns_400_when_name_missing()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);
        await SeedAgentAsync(store, perAppDb, name: "Support");
        var agentId = await FirstAgentIdAsync(store, perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.PostAsJsonAsync(
            "/api/apps/my-app/agent",
            new { identifier = agentId, systemPrompt = "You help.", connectionStringName = "demo-llm" });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    private static async Task<string> FirstAgentIdAsync(IDocumentStore store, string database)
    {
        var agents = await store.Maintenance.ForDatabase(database).SendAsync(new GetAiAgentsOperation());
        return agents.AiAgents![0].Identifier!;
    }
}
