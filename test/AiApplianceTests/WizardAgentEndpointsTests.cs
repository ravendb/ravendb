using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using AiApplianceTests.E2E.Fixtures;
using FastTests;
using Raven.AiAppliance.Wizard;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Tests.Infrastructure;
using Xunit;

namespace AiApplianceTests;

/// W7 + W8 endpoint coverage. W7 = POST /api/apps/{slug}/setup/agent
/// (provision agent against the per-app DB). W8 = POST /api/apps/{slug}/setup/channel
/// (register a channel-instance doc in the app DB per design §3.4). Both are
/// exercised end-to-end in ApplianceFullFlowTests T11/T12; this suite is the
/// focused unit coverage.
///
/// Test isolation: each test uses its own GetDocumentStore() — the store's
/// auto-named DB IS the appliance's config DB for that test. Per-app DBs get
/// a Guid suffix so parallel / serial test runs don't step on each other.
public class WizardAgentEndpointsTests(ITestOutputHelper output) : RavenTestBase(output)
{
    // ---- W7 ----

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Agent_endpoint_returns_404_for_unknown_slug()
    {
        var store = GetDocumentStore();

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.PostAsJsonAsync(
            "/api/apps/nonexistent/setup/agent",
            new { framing = "customer-support" });

        Assert.Equal(HttpStatusCode.NotFound, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Agent_endpoint_returns_agentId_for_known_slug()
    {
        var store = GetDocumentStore();
        var perAppDb = await CreatePerAppDatabaseAsync(store);
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.PostAsJsonAsync(
            "/api/apps/my-app/setup/agent",
            new { framing = "customer-support" });

        Assert.True(resp.IsSuccessStatusCode,
            $"agent returned {resp.StatusCode}: {await resp.Content.ReadAsStringAsync()}");
        var json = await resp.Content.ReadFromJsonAsync<JsonElement>();
        var agentId = json.GetProperty("agentId").GetString();
        Assert.False(string.IsNullOrEmpty(agentId), $"agentId was empty: {json}");
    }

    // ---- W8 ----

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Channel_endpoint_returns_404_for_unknown_slug()
    {
        var store = GetDocumentStore();

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.PostAsJsonAsync(
            "/api/apps/nonexistent/setup/channel",
            new { type = "iframe", agentId = "demo-agent", allowedOrigins = new[] { "http://localhost" } });

        Assert.Equal(HttpStatusCode.NotFound, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Channel_endpoint_returns_widgetId_for_known_app()
    {
        var store = GetDocumentStore();
        var perAppDb = await CreatePerAppDatabaseAsync(store);
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.PostAsJsonAsync(
            "/api/apps/my-app/setup/channel",
            new { type = "iframe", agentId = "demo-agent", allowedOrigins = new[] { "http://localhost" } });

        Assert.True(resp.IsSuccessStatusCode,
            $"channel returned {resp.StatusCode}: {await resp.Content.ReadAsStringAsync()}");
        var json = await resp.Content.ReadFromJsonAsync<JsonElement>();
        var widgetId = json.GetProperty("widgetId").GetString();
        Assert.False(string.IsNullOrEmpty(widgetId), $"widgetId was empty: {json}");
        Assert.StartsWith("wgt_", widgetId);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Channel_endpoint_rejects_unsupported_type()
    {
        var store = GetDocumentStore();
        var perAppDb = await CreatePerAppDatabaseAsync(store);
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.PostAsJsonAsync(
            "/api/apps/my-app/setup/channel",
            new { type = "whatsapp", agentId = "demo-agent", allowedOrigins = Array.Empty<string>() });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    // ---- helpers ----

    private ApplianceWebApplicationFactory NewApplianceFactory(IDocumentStore store) =>
        new(licenseApiUrl: "http://unused-in-unit-tests",
            setupPackagePath: NewDataPath(forceCreateDir: true),
            applianceStore: store,
            configureOptions: opts =>
            {
                // Pin the appliance's "config DB" to the test store's own
                // (auto-named, unique-per-test) database — so parallel /
                // serial tests against the shared RavenDB server don't
                // step on each other's App docs.
                opts.ConfigDatabase = store.Database;

                // Default LlmProvider is "openai" with an empty API key;
                // RavenDB rejects the connection-string put with "ApiKey
                // field cannot be empty". Switching to Ollama (no key
                // required) lets the agent-registration path complete
                // end-to-end against the in-process store without needing
                // a real LLM credential.
                opts.LlmProvider = "ollama";
                opts.LlmEndpoint = "http://localhost:11434/";
                opts.LlmModel = "llama3.1";
            });

    private static async Task<string> CreatePerAppDatabaseAsync(IDocumentStore store)
    {
        var name = "per-app-" + Guid.NewGuid().ToString("N");
        await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(name)));
        return name;
    }

    private static async Task SeedAppAsync(IDocumentStore store, string slug, string database)
    {
        // The "config DB" is store.Database (PostConfigure'd above); seeding
        // the App doc there means the wizard endpoints find it.
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
