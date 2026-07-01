using System.Net;
using System.Net.Http.Json;
using System.Net.WebSockets;
using System.Text.Json;
using AiApplianceTests.E2E.Fixtures;
using FastTests;
using Raven.AiAppliance.Channels;
using Raven.AiAppliance.Endpoints;
using Raven.AiAppliance.Wizard;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Tests.Infrastructure;
using Xunit;

namespace AiApplianceTests;

/// <summary>
/// RavenDB-26700 (T-6) channel lifecycle + wizard read-side coverage:
/// list / edit / delete channels, the <c>/cdc/progress</c> WebSocket, and the
/// <c>/setup/try</c> draft "Test agent" smoke stream, plus the two channel-adjacent
/// embed gates (CSP header + agent-deleted 404). The minted-token embed lifecycle
/// lives in <see cref="EmbedLinksTests"/> (RavenDB-26775). The real-LLM reply path
/// is asserted in the E2E (<see cref="E2E.ApplianceFullFlowTests"/> T14); here we
/// only prove the stream wiring opens with valid NDJSON against the draft, so these
/// run without a live LLM.
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

    // ---- embed page (RavenDB-26775: minted token links) ----
    // The page/chat-by-token lifecycle (renders, malformed-token 404, cap,
    // expiry, revoke, origin matrix, conversation binding) lives in
    // EmbedLinksTests. Here we keep the two channel-adjacent gates: the exact
    // CSP header derived from the channel, and the agent-deleted 404.

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

        // Non-empty origins -> frame-ancestors CSP on the embed page. 'self'
        // is always present so the appliance's own UI can preview the widget.
        await ProvisionIFrameChannelAsync(client, "app-a");
        var restrictedToken = await MintLinkAsync(client, "app-a");
        var restricted = await client.GetAsync($"/embed/{restrictedToken}");
        Assert.Equal(HttpStatusCode.OK, restricted.StatusCode);
        var csp = Assert.Single(restricted.Headers.GetValues("Content-Security-Policy"));
        Assert.Equal($"{EmbedEndpoints.BaseCsp}; frame-ancestors 'self' http://localhost", csp);

        // Empty origins -> the resource CSP is still present, but with NO frame-ancestors: the embed
        // page stays embeddable from anywhere (M1 decision 2026-06-04) while operator CSS stays contained.
        await ApplianceTestSeed.SeedMockAgentAsync(client, "app-b", "demo-agent");
        var openProvision = await client.PostAsJsonAsync("/api/apps/app-b/setup/channel",
            new { type = "iframe", agentId = "demo-agent", allowedOrigins = Array.Empty<string>() });
        Assert.True(openProvision.IsSuccessStatusCode, await openProvision.Content.ReadAsStringAsync());
        var openToken = await MintLinkAsync(client, "app-b");
        var open = await client.GetAsync($"/embed/{openToken}");
        Assert.Equal(HttpStatusCode.OK, open.StatusCode);
        var openCsp = Assert.Single(open.Headers.GetValues("Content-Security-Policy"));
        Assert.Equal(EmbedEndpoints.BaseCsp, openCsp);
        Assert.DoesNotContain("frame-ancestors", openCsp);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Embed_returns_404_when_token_resolves_to_a_non_iframe_channel()
    {
        // The embed surface is iFrame-only: a token whose channel is a non-IFrame
        // type (e.g. Telegram) must be a miss. Covers the channel.Type != IFrame
        // guard in EmbedEndpoints.ResolveAsync. Seed the link + channel directly
        // (the API can't provision a Telegram channel — it 501s).
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        var token = Guid.NewGuid().ToString("N");
        const string widgetId = "wgt_telegram_x";
        using (var cfg = store.OpenAsyncSession())
        {
            await cfg.StoreAsync(new LinkIndex { Id = $"link-index/{token}", Slug = "my-app" });
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
            await session.StoreAsync(new EmbedLink
            {
                Id = $"embed-links/{token}",
                WidgetId = widgetId,
                AgentId = "demo-agent",
                ExpiresAt = DateTime.UtcNow.AddHours(1),
                MaxInvocations = 10,
                CreatedAt = DateTime.UtcNow,
            });
            await session.SaveChangesAsync();
        }

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var page = await client.GetAsync($"/embed/{token}");
        Assert.Equal(HttpStatusCode.NotFound, page.StatusCode);

        var chat = await client.PostAsJsonAsync($"/embed/{token}/chat", new { prompt = "hi" });
        Assert.Equal(HttpStatusCode.NotFound, chat.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Embed_chat_returns_404_for_unregistered_agent()
    {
        // L1 (review 2026-06-04): a link whose channel's AgentId no longer
        // resolves to a persisted agent (e.g. the agent was deleted) must fail
        // with a clean 404 before the NDJSON stream opens — not 200 + an error
        // frame. Seed the link + channel directly with a ghost agent.
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        var token = Guid.NewGuid().ToString("N");
        const string widgetId = "wgt_ghost_agent";
        using (var cfg = store.OpenAsyncSession())
        {
            await cfg.StoreAsync(new LinkIndex { Id = $"link-index/{token}", Slug = "my-app" });
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
            await session.StoreAsync(new EmbedLink
            {
                Id = $"embed-links/{token}",
                WidgetId = widgetId,
                AgentId = "ghost-agent",
                ExpiresAt = DateTime.UtcNow.AddHours(1),
                MaxInvocations = 10,
                CreatedAt = DateTime.UtcNow,
            });
            await session.SaveChangesAsync();
        }

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.PostAsJsonAsync($"/embed/{token}/chat", new { prompt = "hi" });
        Assert.Equal(HttpStatusCode.NotFound, resp.StatusCode);

        // The gate reserves an invocation before the agent lookup; a deleted agent
        // must refund it (Copilot review C2) so the 404 doesn't permanently burn one.
        using (var session = store.OpenAsyncSession(perAppDb))
        {
            var link = await session.LoadAsync<EmbedLink>($"embed-links/{token}");
            Assert.Equal(0, link.InvocationCount);
        }
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
        // cdc/progress is under the gated /api/apps group — carry the operator key on the upgrade.
        wsClient.ConfigureRequest = request =>
            request.Headers[Raven.AiAppliance.Auth.ApiKeyAuthenticationHandler.HeaderName] =
                ApplianceWebApplicationFactory.TestApiKey;
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
    public async Task SetupTry_streams_ndjson_for_the_draft()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        // Seed the connection string the draft references (this also provisions a throwaway
        // agent, which the draft test ignores — it runs the posted configuration, not a
        // persisted agent).
        await ApplianceTestSeed.SeedMockAgentAsync(client, "my-app", "demo-agent");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var req = new HttpRequestMessage(HttpMethod.Post, "/api/apps/my-app/setup/try")
        {
            Content = JsonContent.Create(new
            {
                prompt = "hello",
                configuration = new
                {
                    name = "Draft Agent",
                    systemPrompt = "You are a placeholder draft agent.",
                    connectionStringName = "demo-llm",
                },
            }),
        };
        var resp = await client.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, cts.Token);
        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        Assert.Contains("application/x-ndjson", resp.Content.Headers.ContentType?.ToString() ?? "");

        // No live LLM in this unit test, so the wiring surfaces a valid NDJSON frame: chunk/done
        // if a model happened to be reachable, otherwise an error frame.
        var line = await ReadFirstLineAsync(resp, l => string.IsNullOrWhiteSpace(l) == false, cts.Token);
        Assert.NotNull(line);
        using var doc = JsonDocument.Parse(line!);
        var type = doc.RootElement.GetProperty("type").GetString();
        Assert.Contains(type, new[] { "chunk", "done", "error" });
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task SetupTry_returns_400_when_prompt_missing()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.PostAsJsonAsync(
            "/api/apps/my-app/setup/try",
            new { configuration = new { name = "Draft", systemPrompt = "p", connectionStringName = "demo-llm" } });
        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task SetupTry_returns_400_when_configuration_missing()
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
    public async Task SetupTry_returns_404_for_unknown_slug()
    {
        var store = GetDocumentStore();
        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.PostAsJsonAsync(
            "/api/apps/nonexistent/setup/try",
            new
            {
                prompt = "hi",
                configuration = new { name = "Draft", systemPrompt = "p", connectionStringName = "demo-llm" },
            });
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

    private static async Task<string> MintLinkAsync(HttpClient client, string slug, string agentId = "demo-agent")
    {
        var resp = await client.PostAsJsonAsync($"/api/apps/{slug}/embed-links",
            new { agentId, ttlSeconds = 3600, maxInvocations = 50 });
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());
        var json = await resp.Content.ReadFromJsonAsync<JsonElement>();
        return json.GetProperty("token").GetString()!;
    }

    /// <summary>Reads the streamed response line by line and returns the first
    /// line satisfying <paramref name="match"/>, or null on end-of-stream /
    /// cancellation. Cancellation comes from the caller's CTS so the test never
    /// hangs on the open-ended NDJSON stream.</summary>
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
