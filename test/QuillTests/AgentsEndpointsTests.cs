using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using FastTests;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Quill.Wizard;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

/// <summary>
/// Coverage for the dashboard read-side <c>GET /api/apps/{slug}/agents</c>
/// endpoint: it projects each provisioned RavenDB AI agent to a summary,
/// resolving the model off the referenced connection string. No live LLM is
/// needed — provisioning a connection string + agent are pure maintenance ops.
/// </summary>
public class AgentsEndpointsTests(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Agents_list_returns_provisioned_agent_with_model()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);
        await SeedAgentAsync(store, perAppDb, name: "Support", connectionStringName: "oai", model: "gpt-4o-mini");

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.GetAsync("/api/apps/my-app/agents");
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        var items = await resp.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal(1, items.GetArrayLength());
        var item = items[0];
        Assert.Equal("Support", item.GetProperty("name").GetString());
        Assert.Equal("gpt-4o-mini", item.GetProperty("model").GetString());
        Assert.False(item.GetProperty("disabled").GetBoolean());
        Assert.False(string.IsNullOrEmpty(item.GetProperty("agentId").GetString()));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Agents_list_is_empty_for_app_with_no_agents()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.GetAsync("/api/apps/my-app/agents");
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());
        var items = await resp.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal(0, items.GetArrayLength());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Agents_list_returns_404_for_unknown_slug()
    {
        var store = GetDocumentStore();
        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.GetAsync("/api/apps/nonexistent/agents");
        Assert.Equal(HttpStatusCode.NotFound, resp.StatusCode);
    }

    // ---- helpers ----

    private static async Task SeedAgentAsync(
        IDocumentStore store, string database, string name, string connectionStringName, string model)
    {
        await store.Maintenance.ForDatabase(database).SendAsync(
            new PutConnectionStringOperation<AiConnectionString>(new AiConnectionString
            {
                Name = connectionStringName,
                ModelType = AiModelType.Chat,
                OpenAiSettings = new OpenAiSettings { ApiKey = "sk-test", Model = model },
            }));

        await store.AI.ForDatabase(database).CreateAgentAsync(new AiAgentConfiguration
        {
            Name = name,
            ConnectionStringName = connectionStringName,
            SampleObject = """{"reply":""}""",
        });
    }

    private ApplianceWebApplicationFactory NewApplianceFactory(IDocumentStore store) =>
        new(licenseApiUrl: "http://unused-in-unit-tests",
            setupPackagePath: NewDataPath(forceCreateDir: true),
            applianceStore: store,
            configureOptions: opts => opts.ConfigDatabase = store.Database);

    private async Task<(string Name, IDisposable Cleanup)> CreatePerAppDatabaseAsync(IDocumentStore store)
    {
        var name = "per-app-" + Guid.NewGuid().ToString("N");
        await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(name)));
        return (name, Databases.EnsureDatabaseDeletion(name, store));
    }

    private static async Task SeedAppAsync(IDocumentStore store, string slug, string database)
    {
        using var session = store.OpenAsyncSession();
        await session.StoreAsync(new App
        {
            Slug = slug,
            AppName = slug,
            Database = database,
            CdcTaskName = $"{slug}-cdc",
            CreatedAt = DateTime.UtcNow,
        }, id: $"apps/{slug}");
        await session.SaveChangesAsync();
    }
}
