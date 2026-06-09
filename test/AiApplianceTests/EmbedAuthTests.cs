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

/// <summary>
/// RavenDB-26700 embed chat contract: random chats/{guid} continuation id +
/// the M1b Origin gate. No live LLM — these cover the request gates
/// (400/403/404/410) and that the stream opens; real minted-id + continuation
/// are in the E2E (<see cref="E2E.ApplianceFullFlowTests"/> T14/T14b).
/// </summary>
public class EmbedAuthTests(ITestOutputHelper output) : RavenTestBase(output)
{
    // ---- continuation-id contract ----

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task First_turn_opens_an_ndjson_stream()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();
        var widgetId = await ProvisionIFrameChannelAsync(client, "my-app");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var resp = await SendChatAsync(client, widgetId, new { prompt = "hello" }, cts.Token);

        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        Assert.Contains("application/x-ndjson", resp.Content.Headers.ContentType?.ToString() ?? "");

        // No LLM in a unit run, so the wiring surfaces a valid NDJSON frame
        // (chunk/done if an LLM were reachable, otherwise error).
        var lines = await ReadAllLinesAsync(resp, cts.Token);
        Assert.NotEmpty(lines);
        using var first = JsonDocument.Parse(lines[0]);
        Assert.Contains(first.RootElement.GetProperty("type").GetString(), new[] { "chunk", "done", "error" });
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Continuation_with_a_chats_id_opens_a_stream()
    {
        // Continuation accepts a chats/-prefixed id; the real resumed thread is E2E.
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();
        var widgetId = await ProvisionIFrameChannelAsync(client, "my-app");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var resp = await SendChatAsync(client, widgetId,
            new { prompt = "again", conversationId = "chats/" + Guid.NewGuid().ToString("N") }, cts.Token);

        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        Assert.Contains("application/x-ndjson", resp.Content.Headers.ContentType?.ToString() ?? "");
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Non_chats_conversation_id_is_rejected()
    {
        // A2 guard: a client conversationId is pinned to the chats/ prefix → clean 400.
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();
        var widgetId = await ProvisionIFrameChannelAsync(client, "my-app");

        var resp = await client.PostAsJsonAsync($"/embed/{widgetId}/chat",
            new { prompt = "hi", conversationId = "users/admin" });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    [RavenTheory(RavenTestCategory.AiAppliance)]
    [InlineData("chats/")]    // bare prefix → server auto-allocates a sequential id
    [InlineData("chats/x|")]  // trailing | → cluster identity (chats/1, chats/2…)
    public async Task Conversation_id_that_forces_server_allocation_is_rejected(string conversationId)
    {
        // A2: a client must not be able to force RavenDB to mint an enumerable id.
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();
        var widgetId = await ProvisionIFrameChannelAsync(client, "my-app");

        var resp = await client.PostAsJsonAsync($"/embed/{widgetId}/chat",
            new { prompt = "hi", conversationId });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    // ---- M1b Origin matrix ----

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Origin_check_blocks_disallowed_and_passes_allowed_self_and_absent()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();
        // Allowed list deliberately EXCLUDES the appliance's own origin
        // (http://localhost) so the self-origin rule is what passes it.
        var widgetId = await ProvisionIFrameChannelAsync(client, "my-app", origins: new[] { "http://customer.example" });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));

        var blocked = await SendChatAsync(client, widgetId, new { prompt = "hi" }, cts.Token, origin: "http://evil.example");
        Assert.Equal(HttpStatusCode.Forbidden, blocked.StatusCode);
        Assert.Equal("origin_forbidden", await ReadErrorCodeAsync(blocked));

        var allowed = await SendChatAsync(client, widgetId, new { prompt = "hi" }, cts.Token, origin: "http://customer.example");
        Assert.Equal(HttpStatusCode.OK, allowed.StatusCode);

        // Case-insensitive (RFC 3986): unusual casing on an allowed origin still passes.
        var allowedCased = await SendChatAsync(client, widgetId, new { prompt = "hi" }, cts.Token, origin: "HTTP://CUSTOMER.EXAMPLE");
        Assert.Equal(HttpStatusCode.OK, allowedCased.StatusCode);

        var self = await SendChatAsync(client, widgetId, new { prompt = "hi" }, cts.Token, origin: "http://localhost");
        Assert.Equal(HttpStatusCode.OK, self.StatusCode);

        var absent = await SendChatAsync(client, widgetId, new { prompt = "hi" }, cts.Token);
        Assert.Equal(HttpStatusCode.OK, absent.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Empty_allowed_origins_skips_the_origin_check()
    {
        // M1 contract: explicit [] = embeddable/postable from anywhere.
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();
        var widgetId = await ProvisionIFrameChannelAsync(client, "my-app", origins: Array.Empty<string>());

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var resp = await SendChatAsync(client, widgetId, new { prompt = "hi" }, cts.Token, origin: "http://anywhere.example");
        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
    }

    // ---- gate precedence ----

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Disabled_channel_returns_410()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();
        var widgetId = await ProvisionIFrameChannelAsync(client, "my-app");

        var disable = await client.PutAsJsonAsync($"/api/apps/my-app/channels/{widgetId}", new { enabled = false });
        Assert.True(disable.IsSuccessStatusCode, await disable.Content.ReadAsStringAsync());

        var resp = await client.PostAsJsonAsync($"/embed/{widgetId}/chat", new { prompt = "hi" });
        Assert.Equal(HttpStatusCode.Gone, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Unknown_widget_returns_404()
    {
        var store = GetDocumentStore();
        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.PostAsJsonAsync("/embed/wgt_nope/chat", new { prompt = "hi" });
        Assert.Equal(HttpStatusCode.NotFound, resp.StatusCode);
    }

    // ---- helpers ----

    private static async Task<HttpResponseMessage> SendChatAsync(
        HttpClient client, string widgetId, object body, CancellationToken ct, string? origin = null)
    {
        var req = new HttpRequestMessage(HttpMethod.Post, $"/embed/{widgetId}/chat")
        {
            Content = JsonContent.Create(body),
        };
        if (origin is not null)
            req.Headers.TryAddWithoutValidation("Origin", origin);

        return await client.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, ct);
    }

    /// <summary>Drains the NDJSON stream into its non-empty lines; the caller's
    /// CTS bounds the read so a hung stream fails the test instead of hanging it.</summary>
    private static async Task<List<string>> ReadAllLinesAsync(HttpResponseMessage resp, CancellationToken ct)
    {
        var lines = new List<string>();
        try
        {
            await using var stream = await resp.Content.ReadAsStreamAsync(ct);
            using var reader = new StreamReader(stream);
            while (ct.IsCancellationRequested == false)
            {
                var line = await reader.ReadLineAsync(ct);
                if (line is null)
                    break;
                if (string.IsNullOrWhiteSpace(line) == false)
                    lines.Add(line);
            }
        }
        catch (OperationCanceledException)
        {
            // Timed out draining — return what arrived.
        }

        return lines;
    }

    private static async Task<string?> ReadErrorCodeAsync(HttpResponseMessage resp)
    {
        var json = await resp.Content.ReadFromJsonAsync<JsonElement>();
        return json.GetProperty("code").GetString();
    }

    private static async Task<string> ProvisionIFrameChannelAsync(
        HttpClient client, string slug, string agentId = "demo-agent", string[]? origins = null)
    {
        await SeedMockAgentAsync(client, slug, agentId);

        var resp = await client.PostAsJsonAsync($"/api/apps/{slug}/setup/channel",
            new { type = "iframe", agentId, allowedOrigins = origins ?? new[] { "http://localhost" } });
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());
        var json = await resp.Content.ReadFromJsonAsync<JsonElement>();
        return json.GetProperty("widgetId").GetString()!;
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

    /// <summary>
    /// Seeds a mock connection string + agent in the app's per-app DB so the
    /// channel / embed endpoints (which resolve the agent from the database,
    /// not a compile-time registry) have a real agent to bind to. The Ollama CS
    /// is stored config only — it is never dialed.
    /// </summary>
    private static async Task SeedMockAgentAsync(HttpClient client, string slug = "my-app", string agentId = "demo-agent")
    {
        var csResp = await client.PostAsJsonAsync(
            $"/api/apps/{slug}/ai/connection-strings",
            new
            {
                name = "demo-llm",
                identifier = "demo-llm",
                modelType = "Chat",
                ollamaSettings = new { uri = "http://localhost:11434/", model = "llama3.1" }
            });
        Assert.True(csResp.IsSuccessStatusCode,
            $"seed connection-string returned {csResp.StatusCode}: {await csResp.Content.ReadAsStringAsync()}");

        var agentResp = await client.PostAsJsonAsync(
            $"/api/apps/{slug}/setup/agent",
            new
            {
                identifier = agentId,
                name = "Demo Agent",
                systemPrompt = "You are a placeholder demo agent.",
                connectionStringName = "demo-llm",
            });
        Assert.True(agentResp.IsSuccessStatusCode,
            $"seed agent returned {agentResp.StatusCode}: {await agentResp.Content.ReadAsStringAsync()}");
    }
}
