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
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
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

    [RavenTheory(RavenTestCategory.AiAppliance)]
    [InlineData("demo-agent")]   // valid agentId — still 404 because slug is unknown
    [InlineData("ghost-agent")]  // invalid agentId — L1: must NOT leak via differential 400 vs 404
    public async Task Channel_endpoint_returns_404_for_unknown_slug(string agentId)
    {
        var store = GetDocumentStore();

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.PostAsJsonAsync(
            "/api/apps/nonexistent/setup/channel",
            new { type = "iframe", agentId, allowedOrigins = new[] { "http://localhost" } });

        Assert.Equal(HttpStatusCode.NotFound, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Channel_endpoint_returns_widgetId_for_known_app()
    {
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
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
        // H1: 128-bit random (Base64url-encoded) → exactly 22 chars after the
        // padding is trimmed (16 bytes × 4/3 = 21.33 → 22 chars without padding),
        // plus the 'wgt_' prefix (4) = 26 total. Earlier 32-bit form produced 12;
        // assert against the actual lower bound, not a loose >=24.
        Assert.True(widgetId.Length >= 26,
            $"widgetId length {widgetId.Length} is below the 128-bit-entropy floor (expected ≥26 incl. 'wgt_' prefix): '{widgetId}'");
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Channel_endpoint_rejects_unsupported_type()
    {
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.PostAsJsonAsync(
            "/api/apps/my-app/setup/channel",
            new { type = "whatsapp", agentId = "demo-agent", allowedOrigins = Array.Empty<string>() });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Channel_endpoint_returns_same_widgetId_for_repeated_calls()
    {
        // M3: idempotency. Two POSTs with the same body must return the same
        // widgetId — operator double-click / client retry should not create
        // orphan channel docs.
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var body = new { type = "iframe", agentId = "demo-agent", allowedOrigins = new[] { "http://localhost" } };

        var resp1 = await client.PostAsJsonAsync("/api/apps/my-app/setup/channel", body);
        Assert.True(resp1.IsSuccessStatusCode, await resp1.Content.ReadAsStringAsync());
        var widgetId1 = (await resp1.Content.ReadFromJsonAsync<JsonElement>()).GetProperty("widgetId").GetString();

        // Let the in-process auto-index catch up so the idempotency query on
        // the server side sees the freshly-stored channel. Without this the
        // second POST may read stale and create a second doc.
        Indexes.WaitForIndexing(store, perAppDb);

        var resp2 = await client.PostAsJsonAsync("/api/apps/my-app/setup/channel", body);
        Assert.True(resp2.IsSuccessStatusCode, await resp2.Content.ReadAsStringAsync());
        var widgetId2 = (await resp2.Content.ReadFromJsonAsync<JsonElement>()).GetProperty("widgetId").GetString();

        Assert.Equal(widgetId1, widgetId2);

        // And only one channel doc in the per-app DB.
        using var session = store.OpenAsyncSession(perAppDb);
        var count = await session.Query<ChannelInstance>().CountAsync();
        Assert.Equal(1, count);
    }

    [RavenTheory(RavenTestCategory.AiAppliance)]
    [InlineData("*")]                                  // M2: wildcard widens trust
    [InlineData("example.com")]                        // M2: scheme-less
    [InlineData("ftp://example.com")]                  // M2: non-http(s) scheme
    [InlineData("")]                                   // M2: empty entry
    public async Task Channel_endpoint_rejects_invalid_allowed_origin(string badOrigin)
    {
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.PostAsJsonAsync(
            "/api/apps/my-app/setup/channel",
            new { type = "iframe", agentId = "demo-agent", allowedOrigins = new[] { badOrigin } });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Channel_endpoint_rejects_too_many_allowed_origins()
    {
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        // M2: 33 entries exceeds the 32 cap.
        var tooMany = Enumerable.Range(0, 33)
            .Select(i => $"http://example{i}.com")
            .ToArray();

        var resp = await client.PostAsJsonAsync(
            "/api/apps/my-app/setup/channel",
            new { type = "iframe", agentId = "demo-agent", allowedOrigins = tooMany });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    [RavenTheory(RavenTestCategory.AiAppliance)]
    [InlineData("a\u0007b")]                          // M4: BEL control char (escape-sequence so source is all printable)
    [InlineData("name\twith\ttabs")]                    // M4: tab is also a control char
    public async Task Channel_endpoint_rejects_invalid_display_name(string badName)
    {
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.PostAsJsonAsync(
            "/api/apps/my-app/setup/channel",
            new
            {
                type = "iframe",
                agentId = "demo-agent",
                allowedOrigins = new[] { "http://localhost" },
                displayName = badName,
            });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Channel_endpoint_rejects_too_long_display_name()
    {
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.PostAsJsonAsync(
            "/api/apps/my-app/setup/channel",
            new
            {
                type = "iframe",
                agentId = "demo-agent",
                allowedOrigins = new[] { "http://localhost" },
                displayName = new string('x', 201),  // M4: 200-char cap
            });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Channel_endpoint_persists_canonical_agent_id_casing()
    {
        // L3: AgentSchemaRegistry resolves case-insensitively, but the
        // persisted ChannelInstance.AgentId must adopt the registry's
        // canonical casing — otherwise later case-sensitive queries
        // (e.g. M3's idempotency lookup) break across re-runs that
        // mix casings.
        var store = GetDocumentStore();
        var (perAppDb, perAppDbCleanup) = await CreatePerAppDatabaseAsync(store);
        using var _ = perAppDbCleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.PostAsJsonAsync(
            "/api/apps/my-app/setup/channel",
            new { type = "iframe", agentId = "Demo-Agent", allowedOrigins = new[] { "http://localhost" } });

        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        using var session = store.OpenAsyncSession(perAppDb);
        var channel = await session.Query<ChannelInstance>().FirstAsync();
        Assert.Equal("demo-agent", channel.AgentId);
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

    /// <summary>
    /// Creates a uniquely-named per-app database on the test store and
    /// returns its name plus a cleanup handle. Tests <c>using</c> the
    /// handle so the database drops at test end — otherwise these DBs
    /// accumulate on the (shared) test server across runs and slow the
    /// dev loop down (Copilot review #4361946757 C6).
    /// </summary>
    private async Task<(string Name, IDisposable Cleanup)> CreatePerAppDatabaseAsync(IDocumentStore store)
    {
        var name = "per-app-" + Guid.NewGuid().ToString("N");
        await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(name)));
        return (name, Databases.EnsureDatabaseDeletion(name, store));
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
