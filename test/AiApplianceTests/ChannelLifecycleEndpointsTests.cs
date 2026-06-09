using System.Net;
using System.Net.Http.Json;
using System.Net.WebSockets;
using System.Text.Json;
using AiApplianceTests.E2E.Fixtures;
using FastTests;
using Raven.AiAppliance.Channels;
using Raven.AiAppliance.Wizard;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Tests.Infrastructure;
using Xunit;

namespace AiApplianceTests;

/// <summary>
/// RavenDB-26700 (T-6) channel lifecycle + wizard read-side coverage:
/// list / edit / delete channels, the public <c>/embed/{widgetId}</c> page +
/// its 410-when-disabled behaviour, the <c>/cdc/progress</c> WebSocket, and the
/// <c>/setup/try</c> smoke stream. The real-LLM reply path is asserted in the
/// E2E (<see cref="E2E.ApplianceFullFlowTests"/> T14); here we only prove the
/// stream wiring opens with valid NDJSON, so these run without a live LLM.
/// </summary>
public class ChannelLifecycleEndpointsTests(ITestOutputHelper output) : RavenTestBase(output)
{
    // ---- list ----

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Channels_list_returns_the_created_channel()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var widgetId = await ProvisionIFrameChannelAsync(client, "my-app");

        var resp = await client.GetAsync("/api/apps/my-app/channels");
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        var items = await resp.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal(1, items.GetArrayLength());
        var item = items[0];
        Assert.Equal(widgetId, item.GetProperty("widgetId").GetString());
        Assert.Equal("IFrame", item.GetProperty("type").GetString());
        Assert.Equal("demo-agent", item.GetProperty("agentId").GetString());
        Assert.True(item.GetProperty("enabled").GetBoolean());
        // No secrets / origins / bindingId leaked in the summary.
        Assert.False(item.TryGetProperty("allowedOrigins", out _));
        Assert.False(item.TryGetProperty("bindingId", out _));
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Channels_list_is_empty_for_app_with_no_channels()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.GetAsync("/api/apps/my-app/channels");
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());
        var items = await resp.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal(0, items.GetArrayLength());
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Channels_list_returns_404_for_unknown_slug()
    {
        var store = GetDocumentStore();
        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.GetAsync("/api/apps/nonexistent/channels");
        Assert.Equal(HttpStatusCode.NotFound, resp.StatusCode);
    }

    // ---- edit ----

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Channel_edit_toggles_enabled_and_updates_display_name()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();
        var widgetId = await ProvisionIFrameChannelAsync(client, "my-app");

        var resp = await client.PutAsJsonAsync($"/api/apps/my-app/channels/{widgetId}",
            new { displayName = "Storefront bot", enabled = false });
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        var summary = await resp.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal("Storefront bot", summary.GetProperty("displayName").GetString());
        Assert.False(summary.GetProperty("enabled").GetBoolean());

        using var session = store.OpenAsyncSession(perAppDb);
        var channel = await session.LoadAsync<Channel>($"channels/{widgetId}");
        Assert.Equal("Storefront bot", channel.DisplayName);
        Assert.False(channel.Enabled);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Channel_edit_rejects_invalid_origin()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();
        var widgetId = await ProvisionIFrameChannelAsync(client, "my-app");

        var resp = await client.PutAsJsonAsync($"/api/apps/my-app/channels/{widgetId}",
            new { allowedOrigins = new[] { "not-a-url" } });
        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Channel_edit_returns_404_for_unknown_widget()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.PutAsJsonAsync("/api/apps/my-app/channels/wgt_nope",
            new { enabled = false });
        Assert.Equal(HttpStatusCode.NotFound, resp.StatusCode);
    }

    // ---- delete ----

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Channel_delete_removes_channel_and_binding_and_allows_reprovision()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();
        var widgetId = await ProvisionIFrameChannelAsync(client, "my-app");
        var bindingId = $"channel-bindings/my-app/IFrame/demo-agent";

        var del = await client.DeleteAsync($"/api/apps/my-app/channels/{widgetId}");
        Assert.Equal(HttpStatusCode.NoContent, del.StatusCode);

        using (var session = store.OpenAsyncSession(perAppDb))
        {
            Assert.Null(await session.LoadAsync<Channel>($"channels/{widgetId}"));
            Assert.Null(await session.LoadAsync<ChannelBinding>(bindingId));
        }

        using (var cfg = store.OpenAsyncSession())
            Assert.Null(await cfg.LoadAsync<WidgetIndex>($"widget-index/{widgetId}"));

        // Atomic guard cleared with the binding: the same (slug, type, agentId)
        // tuple provisions cleanly again.
        var reWidgetId = await ProvisionIFrameChannelAsync(client, "my-app");
        Assert.False(string.IsNullOrEmpty(reWidgetId));
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Channel_delete_returns_404_for_unknown_widget()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.DeleteAsync("/api/apps/my-app/channels/wgt_nope");
        Assert.Equal(HttpStatusCode.NotFound, resp.StatusCode);
    }

    // ---- embed page ----

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Embed_page_returns_html_for_enabled_channel()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();
        var widgetId = await ProvisionIFrameChannelAsync(client, "my-app");

        var resp = await client.GetAsync($"/embed/{widgetId}");
        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        Assert.Contains("text/html", resp.Content.Headers.ContentType?.ToString() ?? "");
        var html = await resp.Content.ReadAsStringAsync();
        Assert.Contains(widgetId, html);
        // The chat input must expose an accessible name for screen readers.
        Assert.Contains("aria-label", html);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Embed_page_returns_404_for_unknown_widget()
    {
        var store = GetDocumentStore();
        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.GetAsync("/embed/wgt_nope");
        Assert.Equal(HttpStatusCode.NotFound, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Embed_page_csp_reflects_allowed_origins()
    {
        var store = GetDocumentStore();
        var (dbA, cleanupA) = await CreatePerAppDatabaseAsync(store);
        using var _dbA = cleanupA;
        var (dbB, cleanupB) = await CreatePerAppDatabaseAsync(store);
        using var _dbB = cleanupB;
        await SeedAppAsync(store, slug: "app-a", database: dbA);
        await SeedAppAsync(store, slug: "app-b", database: dbB);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        // Non-empty origins -> frame-ancestors CSP on the embed page.
        var restrictedWidget = await ProvisionIFrameChannelAsync(client, "app-a");
        var restricted = await client.GetAsync($"/embed/{restrictedWidget}");
        Assert.Equal(HttpStatusCode.OK, restricted.StatusCode);
        var csp = Assert.Single(restricted.Headers.GetValues("Content-Security-Policy"));
        Assert.Equal("frame-ancestors http://localhost", csp);

        // Empty origins -> NO CSP header at all: the embed page is intentionally
        // embeddable from anywhere (M1 decision 2026-06-04) until the
        // widget-token work revisits the posture.
        await ApplianceTestSeed.SeedMockAgentAsync(client, "app-b", "demo-agent");
        var openProvision = await client.PostAsJsonAsync("/api/apps/app-b/setup/channel",
            new { type = "iframe", agentId = "demo-agent", allowedOrigins = Array.Empty<string>() });
        Assert.True(openProvision.IsSuccessStatusCode, await openProvision.Content.ReadAsStringAsync());
        var openWidget = (await openProvision.Content.ReadFromJsonAsync<JsonElement>())
            .GetProperty("widgetId").GetString();
        var open = await client.GetAsync($"/embed/{openWidget}");
        Assert.Equal(HttpStatusCode.OK, open.StatusCode);
        Assert.False(open.Headers.Contains("Content-Security-Policy"));
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Disabled_channel_embed_returns_410()
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

        var resp = await client.GetAsync($"/embed/{widgetId}");
        Assert.Equal(HttpStatusCode.Gone, resp.StatusCode);
    }

    // Embed chat continuation + conversationId prefix guard live in EmbedAuthTests.

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Embed_page_returns_404_for_non_iframe_channel()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        // Seed a non-IFrame channel doc + its widget-index pointer directly: the
        // API can't provision Telegram/WhatsApp (they 501), and /embed is the
        // iFrame-only surface, so resolving this widget must be treated as a miss.
        const string widgetId = "wgt_not_iframe";
        using (var cfg = store.OpenAsyncSession())
        {
            await cfg.StoreAsync(new WidgetIndex { Id = $"widget-index/{widgetId}", Slug = "my-app" });
            await cfg.SaveChangesAsync();
        }
        using (var session = store.OpenAsyncSession(perAppDb))
        {
            await session.StoreAsync(new Channel
            {
                Id = $"channels/{widgetId}",
                Type = ChannelType.Telegram,
                AgentId = "demo-agent",
                Enabled = true,
            });
            await session.SaveChangesAsync();
        }

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.GetAsync($"/embed/{widgetId}");
        Assert.Equal(HttpStatusCode.NotFound, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Embed_chat_returns_404_for_unregistered_agent()
    {
        // L1 (review 2026-06-04): a channel whose stored AgentId no longer
        // resolves to a persisted agent (e.g. the agent was deleted) must fail
        // with a clean 404 before the NDJSON stream opens — not 200 + an error
        // frame.
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        const string widgetId = "wgt_ghost_agent";
        using (var cfg = store.OpenAsyncSession())
        {
            await cfg.StoreAsync(new WidgetIndex { Id = $"widget-index/{widgetId}", Slug = "my-app" });
            await cfg.SaveChangesAsync();
        }
        using (var session = store.OpenAsyncSession(perAppDb))
        {
            await session.StoreAsync(new Channel
            {
                Id = $"channels/{widgetId}",
                Type = ChannelType.IFrame,
                AgentId = "ghost-agent",
                Enabled = true,
            });
            await session.SaveChangesAsync();
        }

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.PostAsJsonAsync($"/embed/{widgetId}/chat", new { prompt = "hi" });
        Assert.Equal(HttpStatusCode.NotFound, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Channel_lifecycle_provision_update_delete_reprovision()
    {
        // L2 (review 2026-06-04): the only flow mixing a node-local PUT with a
        // later cluster-wide DELETE of the atomic-guarded docs. Pins that the
        // guard clears and the same (type, agent) tuple re-provisions as a
        // brand-new channel.
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var widgetId = await ProvisionIFrameChannelAsync(client, "my-app");

        var update = await client.PutAsJsonAsync($"/api/apps/my-app/channels/{widgetId}", new { enabled = false });
        Assert.True(update.IsSuccessStatusCode, await update.Content.ReadAsStringAsync());

        var delete = await client.DeleteAsync($"/api/apps/my-app/channels/{widgetId}");
        Assert.Equal(HttpStatusCode.NoContent, delete.StatusCode);

        var gone = await client.GetAsync($"/embed/{widgetId}");
        Assert.Equal(HttpStatusCode.NotFound, gone.StatusCode);

        var reResp = await client.PostAsJsonAsync("/api/apps/my-app/setup/channel",
            new { type = "iframe", agentId = "demo-agent", allowedOrigins = new[] { "http://localhost" } });
        Assert.True(reResp.IsSuccessStatusCode, await reResp.Content.ReadAsStringAsync());
        var reJson = await reResp.Content.ReadFromJsonAsync<JsonElement>();
        Assert.False(reJson.GetProperty("existing").GetBoolean());
        Assert.NotEqual(widgetId, reJson.GetProperty("widgetId").GetString());
    }

    // ---- cdc/progress WebSocket ----

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task CdcProgress_relays_a_live_frame_over_websocket()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);

        var wsClient = factory.Server.CreateWebSocketClient();
        var wsUri = new Uri(factory.Server.BaseAddress, "api/apps/my-app/cdc/progress");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        using var ws = await wsClient.ConnectAsync(wsUri, cts.Token);

        // The bridge proxies RavenDB's cdc-sink/performance/live feed, which
        // emits a heartbeat every ~4s even with no CDC task — so at least one
        // (non-close) frame relays through.
        var buffer = new byte[16 * 1024];
        var result = await ws.ReceiveAsync(new ArraySegment<byte>(buffer), cts.Token);
        Assert.NotEqual(WebSocketMessageType.Close, result.MessageType);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task CdcProgress_returns_404_for_unknown_slug()
    {
        var store = GetDocumentStore();
        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        // Plain GET (no WS upgrade): the app-not-found check runs first -> 404.
        var resp = await client.GetAsync("/api/apps/nonexistent/cdc/progress");
        Assert.Equal(HttpStatusCode.NotFound, resp.StatusCode);
    }

    // ---- setup/try ----

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task SetupTry_opens_an_ndjson_stream()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        await ApplianceTestSeed.SeedMockAgentAsync(client, "my-app", "demo-agent");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var req = new HttpRequestMessage(HttpMethod.Post, "/api/apps/my-app/setup/try")
        {
            Content = JsonContent.Create(new { prompt = "hello", agentId = "demo-agent" }),
        };
        var resp = await client.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, cts.Token);
        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        Assert.Contains("application/x-ndjson", resp.Content.Headers.ContentType?.ToString() ?? "");

        // No live LLM in this unit test, so the wiring surfaces a valid NDJSON
        // frame (chunk/done if an LLM happened to be reachable, otherwise error).
        var line = await ReadFirstLineAsync(resp, l => string.IsNullOrWhiteSpace(l) == false, cts.Token);
        Assert.NotNull(line);
        using var doc = JsonDocument.Parse(line!);
        var type = doc.RootElement.GetProperty("type").GetString();
        Assert.Contains(type, new[] { "chunk", "done", "error" });
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task SetupTry_returns_400_when_agentId_missing()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.PostAsJsonAsync("/api/apps/my-app/setup/try", new { prompt = "hi" });
        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task SetupTry_returns_400_for_unknown_agentId()
    {
        // A non-empty but unknown agentId is a client error, so the handler
        // validates it against the per-app database and returns 400 before
        // opening the NDJSON stream (rather than a 200 + error frame after the
        // headers flush).
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.PostAsJsonAsync(
            "/api/apps/my-app/setup/try",
            new { prompt = "hi", agentId = "does-not-exist" });
        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task SetupTry_returns_404_for_unknown_slug()
    {
        var store = GetDocumentStore();
        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.PostAsJsonAsync("/api/apps/nonexistent/setup/try", new { prompt = "hi", agentId = "demo-agent" });
        Assert.Equal(HttpStatusCode.NotFound, resp.StatusCode);
    }

    // ---- helpers ----

    private static async Task<string> ProvisionIFrameChannelAsync(HttpClient client, string slug, string agentId = "demo-agent")
    {
        await ApplianceTestSeed.SeedMockAgentAsync(client, slug, agentId);

        var resp = await client.PostAsJsonAsync($"/api/apps/{slug}/setup/channel",
            new { type = "iframe", agentId, allowedOrigins = new[] { "http://localhost" } });
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());
        var json = await resp.Content.ReadFromJsonAsync<JsonElement>();
        return json.GetProperty("widgetId").GetString()!;
    }

    /// <summary>Reads the streamed response line by line and returns the first
    /// line satisfying <paramref name="match"/>, or null on end-of-stream /
    /// cancellation. Cancellation comes from the caller's CTS so the test never
    /// hangs on the open-ended SSE / NDJSON stream.</summary>
    private static async Task<string?> ReadFirstLineAsync(HttpResponseMessage resp, Func<string, bool> match, CancellationToken ct)
    {
        try
        {
            await using var stream = await resp.Content.ReadAsStreamAsync(ct);
            using var reader = new StreamReader(stream);
            while (ct.IsCancellationRequested == false)
            {
                var line = await reader.ReadLineAsync(ct);
                if (line is null)
                    break;
                if (match(line))
                    return line;
            }
        }
        catch (OperationCanceledException)
        {
            // Timed out waiting for a matching line.
        }

        return null;
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
