using System.Net;
using System.Net.Http.Json;
using System.Text.Json.Nodes;
using FastTests;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Quill.Wizard;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class AiHelperSuggestAgentEndpointTests(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task From_data_returns_candidates_and_sends_the_cdc_config()
    {
        var store = GetDocumentStore();
        await using var mockAi = await MockAiApi.StartAsync();
        mockAi.AgentResponse = (200, AiHelperSamples.AgentEnvelope(
            AiHelperSamples.BuildAgentConfig(), AiHelperSamples.BuildAgentConfig()));

        using var appCleanup = await SeedProvisionedAppAsync(store, "shop", withCdc: true);

        using var factory = NewApplianceFactory(store, mockAi.BaseAddress);
        var client = factory.CreateClient();

        var resp = await client.PostAsJsonAsync("/api/apps/shop/suggest/agent", new { mode = "from-data" });
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        var node = JsonNode.Parse(await resp.Content.ReadAsStringAsync())!;
        Assert.Equal("Success", (string?)node["status"]);
        Assert.Equal(2, node["configurations"]!.AsArray().Count);

        var sent = JsonNode.Parse(mockAi.LastAgentRequestBody!)!;
        Assert.Equal("CdcBasedAgentConfigSetup", (string?)sent["OperationType"]);
        Assert.Equal("from-data", (string?)sent["Mode"]);
        Assert.NotNull(sent["CdcConfig"]);

        // License + CertificateThumbprint are injected by the bundled RavenDB /assistant/assist
        // proxy now, so the appliance must NOT attach them itself.
        Assert.Null(sent["License"]);
        Assert.Null(sent["CertificateThumbprint"]);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task From_prompt_returns_a_single_candidate()
    {
        var store = GetDocumentStore();
        await using var mockAi = await MockAiApi.StartAsync();
        mockAi.AgentResponse = (200, AiHelperSamples.AgentEnvelope(AiHelperSamples.BuildAgentConfig()));

        using var appCleanup = await SeedProvisionedAppAsync(store, "shop", withCdc: false);

        using var factory = NewApplianceFactory(store, mockAi.BaseAddress);
        var client = factory.CreateClient();

        var resp = await client.PostAsJsonAsync("/api/apps/shop/suggest/agent",
            new { mode = "from-prompt", intentPrompt = "help shoppers find orders" });
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        var node = JsonNode.Parse(await resp.Content.ReadAsStringAsync())!;
        Assert.Single(node["configurations"]!.AsArray());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Unknown_slug_returns_404()
    {
        var store = GetDocumentStore();
        await using var mockAi = await MockAiApi.StartAsync();

        using var factory = NewApplianceFactory(store, mockAi.BaseAddress);
        var client = factory.CreateClient();

        var resp = await client.PostAsJsonAsync("/api/apps/missing/suggest/agent", new { mode = "from-data" });
        Assert.Equal(HttpStatusCode.NotFound, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Rejects_unknown_mode()
    {
        var store = GetDocumentStore();
        await using var mockAi = await MockAiApi.StartAsync();
        using var appCleanup = await SeedProvisionedAppAsync(store, "shop", withCdc: true);

        using var factory = NewApplianceFactory(store, mockAi.BaseAddress);
        var client = factory.CreateClient();

        var resp = await client.PostAsJsonAsync("/api/apps/shop/suggest/agent", new { mode = "sideways" });
        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task From_data_without_cdc_config_returns_400()
    {
        var store = GetDocumentStore();
        await using var mockAi = await MockAiApi.StartAsync();
        using var appCleanup = await SeedProvisionedAppAsync(store, "shop", withCdc: false);

        using var factory = NewApplianceFactory(store, mockAi.BaseAddress);
        var client = factory.CreateClient();

        var resp = await client.PostAsJsonAsync("/api/apps/shop/suggest/agent", new { mode = "from-data" });
        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Returns_422_when_candidate_is_structurally_invalid()
    {
        var store = GetDocumentStore();
        await using var mockAi = await MockAiApi.StartAsync();

        // Candidate missing the required SystemPrompt.
        var invalid = new AiAgentConfiguration { Identifier = "x", Name = "y" };
        mockAi.AgentResponse = (200, AiHelperSamples.AgentEnvelope(invalid));

        using var appCleanup = await SeedProvisionedAppAsync(store, "shop", withCdc: true);

        using var factory = NewApplianceFactory(store, mockAi.BaseAddress);
        var client = factory.CreateClient();

        var resp = await client.PostAsJsonAsync("/api/apps/shop/suggest/agent", new { mode = "from-data" });
        Assert.Equal(HttpStatusCode.UnprocessableEntity, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Candidate_without_identifier_is_still_a_valid_draft()
    {
        var store = GetDocumentStore();
        await using var mockAi = await MockAiApi.StartAsync();

        // No Identifier: provisioning server-assigns one, so drafts without it must be accepted.
        // Only Name + SystemPrompt are required for a draft.
        var noId = new AiAgentConfiguration { Name = "Support", SystemPrompt = "You help." };
        mockAi.AgentResponse = (200, AiHelperSamples.AgentEnvelope(noId));

        using var appCleanup = await SeedProvisionedAppAsync(store, "shop", withCdc: true);

        using var factory = NewApplianceFactory(store, mockAi.BaseAddress);
        var client = factory.CreateClient();

        var resp = await client.PostAsJsonAsync("/api/apps/shop/suggest/agent", new { mode = "from-data" });
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        var node = JsonNode.Parse(await resp.Content.ReadAsStringAsync())!;
        Assert.Single(node["configurations"]!.AsArray());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task From_data_caps_candidates_at_three()
    {
        var store = GetDocumentStore();
        await using var mockAi = await MockAiApi.StartAsync();
        mockAi.AgentResponse = (200, AiHelperSamples.AgentEnvelope(
            AiHelperSamples.BuildAgentConfig(), AiHelperSamples.BuildAgentConfig(),
            AiHelperSamples.BuildAgentConfig(), AiHelperSamples.BuildAgentConfig()));

        using var appCleanup = await SeedProvisionedAppAsync(store, "shop", withCdc: true);

        using var factory = NewApplianceFactory(store, mockAi.BaseAddress);
        var client = factory.CreateClient();

        var resp = await client.PostAsJsonAsync("/api/apps/shop/suggest/agent", new { mode = "from-data" });
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        var node = JsonNode.Parse(await resp.Content.ReadAsStringAsync())!;
        Assert.Equal(3, node["configurations"]!.AsArray().Count);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task From_prompt_caps_candidates_at_one()
    {
        var store = GetDocumentStore();
        await using var mockAi = await MockAiApi.StartAsync();
        mockAi.AgentResponse = (200, AiHelperSamples.AgentEnvelope(
            AiHelperSamples.BuildAgentConfig(), AiHelperSamples.BuildAgentConfig()));

        using var appCleanup = await SeedProvisionedAppAsync(store, "shop", withCdc: false);

        using var factory = NewApplianceFactory(store, mockAi.BaseAddress);
        var client = factory.CreateClient();

        var resp = await client.PostAsJsonAsync("/api/apps/shop/suggest/agent",
            new { mode = "from-prompt", intentPrompt = "help shoppers" });
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        var node = JsonNode.Parse(await resp.Content.ReadAsStringAsync())!;
        Assert.Single(node["configurations"]!.AsArray());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task From_data_consent_required_then_signs_consent_and_retries_successfully()
    {
        var store = GetDocumentStore();
        await using var mockAi = await MockAiApi.StartAsync();
        // The real service 401s with ConsentRequired until consent is signed; mirror that gate.
        mockAi.RequireConsentForAssist = true;
        mockAi.AgentResponse = (200, AiHelperSamples.AgentEnvelope(AiHelperSamples.BuildAgentConfig()));

        using var appCleanup = await SeedProvisionedAppAsync(store, "shop", withCdc: true);

        using var factory = NewApplianceFactory(store, mockAi.BaseAddress);
        var client = factory.CreateClient();

        var resp = await client.PostAsJsonAsync("/api/apps/shop/suggest/agent", new { mode = "from-data" });
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        var node = JsonNode.Parse(await resp.Content.ReadAsStringAsync())!;
        Assert.Equal("Success", (string?)node["status"]);
        Assert.Single(node["configurations"]!.AsArray());

        // Consent was signed once, then the assist was retried and succeeded.
        Assert.Equal(1, mockAi.GiveConsentCallCount);
    }

    private async Task<IDisposable> SeedProvisionedAppAsync(IDocumentStore store, string slug, bool withCdc)
    {
        var perAppDb = "app-" + Guid.NewGuid().ToString("N");
        await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(perAppDb)));
        // Per-app DBs live on the shared test server after the store is disposed; register
        // for deletion to prevent accumulation across runs (mirrors CreatePerAppDatabaseAsync).
        var cleanup = Databases.EnsureDatabaseDeletion(perAppDb, store);

        if (withCdc)
        {
            // CdcSinks can't be seeded via DatabaseRecord (server rejects it); use the
            // dedicated AddCdcSink op. Disabled + SkipInitialLoad keep the CDC process from
            // starting, so no live source connection is attempted. The config lands in
            // DatabaseRecord.CdcSinks for the endpoint to read.
            await store.Maintenance.ForDatabase(perAppDb).SendAsync(
                new PutConnectionStringOperation<SqlConnectionString>(new SqlConnectionString
                {
                    Name = "src",
                    FactoryName = "Npgsql",
                    ConnectionString = "Host=localhost;Database=src",
                }));

            var cdc = AiHelperSamples.BuildCdcConfig();
            cdc.ConnectionStringName = "src";
            cdc.Disabled = true;
            cdc.SkipInitialLoad = true;
            await store.Maintenance.ForDatabase(perAppDb).SendAsync(new AddCdcSinkOperation(cdc));
        }

        using var session = store.OpenAsyncSession();
        await session.StoreAsync(new App
        {
            Slug = slug,
            AppName = slug,
            Database = perAppDb,
            CdcTaskName = $"{slug}-cdc",
            CreatedAt = DateTime.UtcNow,
        }, id: $"apps/{slug}");
        await session.SaveChangesAsync();

        return cleanup;
    }

    private ApplianceWebApplicationFactory NewApplianceFactory(IDocumentStore store, string aiApiUrl)
    {
        var setupPath = NewDataPath(forceCreateDir: true);

        return new ApplianceWebApplicationFactory(
            licenseApiUrl: "http://unused-in-unit-tests",
            setupPackagePath: setupPath,
            applianceStore: store,
            configureOptions: opts =>
            {
                opts.ConfigDatabase = store.Database;
                opts.AiApiUrl = aiApiUrl;
            });
    }
}
