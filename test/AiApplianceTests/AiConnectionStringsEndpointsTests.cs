using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using AiApplianceTests.E2E.Fixtures;
using FastTests;
using Raven.AiAppliance.Wizard;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Tests.Infrastructure;
using Xunit;

namespace AiApplianceTests;

/// AI connection-string CRUD endpoints under /api/apps/{slug}/ai/connection-strings.
/// W7 (provision-agent) now references a connection string by name; these endpoints
/// own the create/list/get/delete lifecycle. Both POST and W7 gate ModelType=Chat
/// and provider in {OpenAi, Ollama} — defence in depth, since a CS created via
/// direct RavenDB Studio could bypass the POST-time gate.
public class AiConnectionStringsEndpointsTests(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Post_creates_ollama_connection_string()
    {
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.PostAsJsonAsync(
            "/api/apps/my-app/ai/connection-strings",
            new
            {
                name = "demo-llm",
                identifier = "demo-llm",
                modelType = "Chat",
                ollamaSettings = new { uri = "http://localhost:11434/", model = "llama3.1" }
            });

        Assert.True(resp.IsSuccessStatusCode,
            $"POST returned {resp.StatusCode}: {await resp.Content.ReadAsStringAsync()}");

        var json = await resp.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal("demo-llm", json.GetProperty("name").GetString());

        // Confirm the CS landed in the per-app DB.
        var cs = await store.Maintenance.ForDatabase(perAppDb).SendAsync(
            new GetConnectionStringsOperation("demo-llm", ConnectionStringType.Ai));
        Assert.NotNull(cs.AiConnectionStrings);
        Assert.True(cs.AiConnectionStrings.ContainsKey("demo-llm"));
        var aiCs = cs.AiConnectionStrings["demo-llm"];
        Assert.Equal(AiModelType.Chat, aiCs.ModelType);
        Assert.NotNull(aiCs.OllamaSettings);
        Assert.Equal("llama3.1", aiCs.OllamaSettings.Model);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Post_returns_400_for_empty_body()
    {
        // C1 from Copilot review #4362803113: minimal APIs can bind an empty
        // request body as null. Without a defensive null-check the handler
        // dereferences body.Name and 500s; this asserts the 400 short-circuit.
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        // Empty body, application/json content type. ASP.NET minimal API
        // binding for a non-nullable complex param 400s before the handler
        // — verifies the framework default is wired up correctly.
        var emptyResp = await client.PostAsync(
            "/api/apps/my-app/ai/connection-strings",
            new StringContent("", System.Text.Encoding.UTF8, "application/json"));
        Assert.Equal(HttpStatusCode.BadRequest, emptyResp.StatusCode);

        // Literal `null` JSON body. The binder parses this as null, hands
        // the null to the handler, and without an explicit null-check the
        // handler dereferences body.Name → NRE → 500. This is the hole
        // Copilot's C1 flagged.
        var nullResp = await client.PostAsync(
            "/api/apps/my-app/ai/connection-strings",
            new StringContent("null", System.Text.Encoding.UTF8, "application/json"));
        Assert.Equal(HttpStatusCode.BadRequest, nullResp.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Post_rejects_empty_name()
    {
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.PostAsJsonAsync(
            "/api/apps/my-app/ai/connection-strings",
            new
            {
                name = "",
                identifier = "demo-llm",
                modelType = "Chat",
                ollamaSettings = new { uri = "http://localhost:11434/", model = "llama3.1" }
            });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Post_rejects_non_chat_model_type()
    {
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.PostAsJsonAsync(
            "/api/apps/my-app/ai/connection-strings",
            new
            {
                name = "embed-llm",
                identifier = "embed-llm",
                modelType = "TextEmbeddings",
                ollamaSettings = new { uri = "http://localhost:11434/", model = "nomic-embed-text" }
            });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Post_rejects_unsupported_provider_in_demo()
    {
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        // MistralAi is a fully-valid RavenDB provider, but the 8-week demo
        // hasn't smoke-tested it end-to-end. Gate at intake so an operator
        // doesn't wire an agent to a provider we can't yet support.
        var resp = await client.PostAsJsonAsync(
            "/api/apps/my-app/ai/connection-strings",
            new
            {
                name = "mistral-llm",
                identifier = "mistral-llm",
                modelType = "Chat",
                mistralAiSettings = new { apiKey = "mistral-key", endpoint = "https://api.mistral.ai/v1/", model = "mistral-tiny" }
            });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Post_rejects_multiple_providers()
    {
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        // Both openAi and ollama settings set — AiConnectionString.ValidateImpl
        // enforces "exactly one provider". Surface the error as 400, not 500.
        var resp = await client.PostAsJsonAsync(
            "/api/apps/my-app/ai/connection-strings",
            new
            {
                name = "mixed-llm",
                identifier = "mixed-llm",
                modelType = "Chat",
                openAiSettings = new { apiKey = "sk-test", endpoint = "https://api.openai.com/v1/", model = "gpt-4o-mini" },
                ollamaSettings = new { uri = "http://localhost:11434/", model = "llama3.1" }
            });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Post_rejects_empty_openai_api_key()
    {
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        // OpenAiBaseSettings.ValidateFields rejects empty ApiKey. Caught by the
        // base ConnectionString.Validate() call; we just need to surface it.
        var resp = await client.PostAsJsonAsync(
            "/api/apps/my-app/ai/connection-strings",
            new
            {
                name = "openai-llm",
                identifier = "openai-llm",
                modelType = "Chat",
                openAiSettings = new { apiKey = "", endpoint = "https://api.openai.com/v1/", model = "gpt-4o-mini" }
            });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Post_returns_404_for_unknown_slug()
    {
        var store = GetDocumentStore();

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.PostAsJsonAsync(
            "/api/apps/nonexistent/ai/connection-strings",
            new
            {
                name = "demo-llm",
                identifier = "demo-llm",
                modelType = "Chat",
                ollamaSettings = new { uri = "http://localhost:11434/", model = "llama3.1" }
            });

        Assert.Equal(HttpStatusCode.NotFound, resp.StatusCode);
    }

    // ---- GET list ----

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task List_returns_created_connection_strings()
    {
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        await PostOllamaCsAsync(client, "my-app", "demo-llm");
        await PostOllamaCsAsync(client, "my-app", "ops-llm");

        var listResp = await client.GetAsync("/api/apps/my-app/ai/connection-strings");
        Assert.True(listResp.IsSuccessStatusCode, await listResp.Content.ReadAsStringAsync());

        var json = await listResp.Content.ReadFromJsonAsync<JsonElement>();
        var items = json.GetProperty("items").EnumerateArray()
            .Select(e => e.GetProperty("name").GetString())
            .OrderBy(n => n)
            .ToArray();

        Assert.Equal(new[] { "demo-llm", "ops-llm" }, items);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task List_returns_404_for_unknown_slug()
    {
        var store = GetDocumentStore();

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.GetAsync("/api/apps/nonexistent/ai/connection-strings");
        Assert.Equal(HttpStatusCode.NotFound, resp.StatusCode);
    }

    // ---- GET by name ----

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task GetByName_returns_ollama_connection_string()
    {
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        await PostOllamaCsAsync(client, "my-app", "demo-llm");

        var resp = await client.GetAsync("/api/apps/my-app/ai/connection-strings/demo-llm");
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        var json = await resp.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal("demo-llm", json.GetProperty("name").GetString());
        Assert.Equal("llama3.1", json.GetProperty("ollamaSettings").GetProperty("model").GetString());
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task GetByName_redacts_openai_api_key()
    {
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        // Persist a real-looking key so the redaction has something to hide.
        var postResp = await client.PostAsJsonAsync(
            "/api/apps/my-app/ai/connection-strings",
            new
            {
                name = "openai-llm",
                identifier = "openai-llm",
                modelType = "Chat",
                openAiSettings = new { apiKey = "sk-real-test-key", endpoint = "https://api.openai.com/v1/", model = "gpt-4o-mini" }
            });
        Assert.True(postResp.IsSuccessStatusCode, await postResp.Content.ReadAsStringAsync());

        var resp = await client.GetAsync("/api/apps/my-app/ai/connection-strings/openai-llm");
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        var json = await resp.Content.ReadFromJsonAsync<JsonElement>();
        var apiKey = json.GetProperty("openAiSettings").GetProperty("apiKey").GetString();
        Assert.Equal("***", apiKey);
        // Non-secret fields untouched.
        Assert.Equal("gpt-4o-mini", json.GetProperty("openAiSettings").GetProperty("model").GetString());
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task GetByName_returns_404_for_unknown_name()
    {
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.GetAsync("/api/apps/my-app/ai/connection-strings/ghost-llm");
        Assert.Equal(HttpStatusCode.NotFound, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task GetByName_returns_404_for_unknown_slug()
    {
        var store = GetDocumentStore();

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.GetAsync("/api/apps/nonexistent/ai/connection-strings/demo-llm");
        Assert.Equal(HttpStatusCode.NotFound, resp.StatusCode);
    }

    // ---- DELETE ----

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Delete_removes_connection_string()
    {
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        await PostOllamaCsAsync(client, "my-app", "demo-llm");

        var deleteResp = await client.DeleteAsync("/api/apps/my-app/ai/connection-strings/demo-llm");
        Assert.Equal(HttpStatusCode.NoContent, deleteResp.StatusCode);

        var getResp = await client.GetAsync("/api/apps/my-app/ai/connection-strings/demo-llm");
        Assert.Equal(HttpStatusCode.NotFound, getResp.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Delete_returns_404_for_unknown_name()
    {
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.DeleteAsync("/api/apps/my-app/ai/connection-strings/ghost-llm");
        Assert.Equal(HttpStatusCode.NotFound, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Delete_returns_404_for_unknown_slug()
    {
        var store = GetDocumentStore();

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.DeleteAsync("/api/apps/nonexistent/ai/connection-strings/demo-llm");
        Assert.Equal(HttpStatusCode.NotFound, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Delete_returns_409_when_referenced_by_agent()
    {
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        // CS exists -> agent created referencing it -> DELETE must block with
        // 409 and surface the agent identifier so the dashboard can render
        // "remove agent(s) first".
        await PostOllamaCsAsync(client, "my-app", "demo-llm");

        var agentResp = await client.PostAsJsonAsync(
            "/api/apps/my-app/setup/agent",
            new { connectionStringName = "demo-llm", framing = "customer-support" });
        Assert.True(agentResp.IsSuccessStatusCode, await agentResp.Content.ReadAsStringAsync());

        var agentId = (await agentResp.Content.ReadFromJsonAsync<JsonElement>())
            .GetProperty("agentId").GetString();

        var deleteResp = await client.DeleteAsync("/api/apps/my-app/ai/connection-strings/demo-llm");
        Assert.Equal(HttpStatusCode.Conflict, deleteResp.StatusCode);

        var body = await deleteResp.Content.ReadAsStringAsync();
        Assert.Contains(agentId!, body);
    }

    // ---- helpers ----

    private static async Task PostOllamaCsAsync(HttpClient client, string slug, string name)
    {
        var resp = await client.PostAsJsonAsync(
            $"/api/apps/{slug}/ai/connection-strings",
            new
            {
                name,
                identifier = name,
                modelType = "Chat",
                ollamaSettings = new { uri = "http://localhost:11434/", model = "llama3.1" }
            });
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());
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
