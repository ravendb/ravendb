using System.Net;
using System.Net.Http.Json;
using System.Net.WebSockets;
using System.Text.Json;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Auth;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Raven.Quill.Endpoints;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class ChannelLifecycleEndpointsTests(ITestOutputHelper output) : QuillTestBase(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Channels_list_returns_the_created_channel()
    {
        await using var app = await NewAppAsync();
        var channelId = await ProvisionIFrameChannelAsync(app);

        var resp = await Host.Client.GetAsync(QuillRoutes.Channels(app.Slug));
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        var items = await resp.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal(1, items.GetArrayLength());
        var item = items[0];
        Assert.Equal(channelId, item.GetProperty("channelId").GetString());
        Assert.Equal("IFrame", item.GetProperty("type").GetString());
        Assert.Equal("demo-agent", item.GetProperty("agentId").GetString());
        Assert.True(item.GetProperty("enabled").GetBoolean());
        var origins = item.GetProperty("allowedOrigins");
        Assert.Equal(1, origins.GetArrayLength());
        Assert.Equal("http://localhost", origins[0].GetString());
        Assert.False(item.TryGetProperty("bindingId", out _));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Channels_list_is_empty_for_app_with_no_channels()
    {
        await using var app = await NewAppAsync();

        var items = await app.GetChannelsAsync();
        Assert.Empty(items);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Channels_list_returns_404_for_unknown_slug()
    {
        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => Host.GetChannelsAsync("nonexistent"));
        Assert.Equal(HttpStatusCode.NotFound, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Channel_edit_toggles_enabled_and_updates_display_name()
    {
        await using var app = await NewAppAsync();
        var channelId = await ProvisionIFrameChannelAsync(app);

        var summary = await app.UpdateChannelAsync(channelId, new UpdateChannelRequest("Storefront bot", null, Enabled: false));
        Assert.Equal("Storefront bot", summary.DisplayName);
        Assert.False(summary.Enabled);

        using var session = app.Store.OpenAsyncSession(app.Slug);
        var channel = await session.LoadAsync<Channel>($"channels/{channelId}");
        Assert.Equal("Storefront bot", channel.DisplayName);
        Assert.False(channel.Enabled);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Channel_edit_rejects_invalid_origin()
    {
        await using var app = await NewAppAsync();
        var channelId = await ProvisionIFrameChannelAsync(app);

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => app.UpdateChannelAsync(channelId, new UpdateChannelRequest(null, new[] { "not-a-url" }, null)));
        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Channel_edit_returns_404_for_unknown_widget()
    {
        await using var app = await NewAppAsync();

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => app.UpdateChannelAsync("nope", new UpdateChannelRequest(null, null, false)));
        Assert.Equal(HttpStatusCode.NotFound, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Channel_delete_removes_channel_and_allows_reprovision()
    {
        await using var app = await NewAppAsync();
        var channelId = await ProvisionIFrameChannelAsync(app);

        using (var session = app.Store.OpenAsyncSession(app.Slug))
        {
            var bindings = await session.Advanced.LoadStartingWithAsync<object>("channel-bindings/");
            Assert.Empty(bindings);
        }

        await app.DeleteChannelAsync(channelId);

        using (var session = app.Store.OpenAsyncSession(app.Slug))
        {
            Assert.Null(await session.LoadAsync<Channel>($"channels/{channelId}"));
        }

        var reChannel = await app.ProvisionChannelAsync(new ProvisionChannelRequest(ChannelType.IFrame, "demo-agent", new[] { "http://localhost" }));
        Assert.False(string.IsNullOrEmpty(reChannel.ChannelId));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Channel_delete_returns_404_for_unknown_widget()
    {
        await using var app = await NewAppAsync();

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => app.DeleteChannelAsync("nope"));
        Assert.Equal(HttpStatusCode.NotFound, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Embed_page_csp_reflects_allowed_origins()
    {
        await using var appA = await NewAppAsync();
        await using var appB = await NewAppAsync();

        var restrictedChannelId = await ProvisionIFrameChannelAsync(appA);
        var restrictedToken = await MintLinkAsync(appA, restrictedChannelId);

        // raw: asserts the CSP response header, which the string-body wrapper can't expose.
        var restricted = await Host.Client.GetAsync(QuillRoutes.EmbedPage(appA.Slug, restrictedToken));
        Assert.Equal(HttpStatusCode.OK, restricted.StatusCode);
        var csp = Assert.Single(restricted.Headers.GetValues("Content-Security-Policy"));
        Assert.EndsWith("frame-ancestors 'self' http://localhost", csp);

        await SeedDemoAgentAsync(appB);
        var openChannel = await appB.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.IFrame, "demo-agent", Array.Empty<string>()));
        var openToken = await MintLinkAsync(appB, openChannel.ChannelId);

        var open = await Host.Client.GetAsync(QuillRoutes.EmbedPage(appB.Slug, openToken));
        Assert.Equal(HttpStatusCode.OK, open.StatusCode);
        var openCsp = Assert.Single(open.Headers.GetValues("Content-Security-Policy"));
        Assert.DoesNotContain("frame-ancestors", openCsp);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Embed_returns_404_when_token_resolves_to_a_non_iframe_channel()
    {
        // Seed the link + channel directly: provisioning a Telegram channel validates the bot
        // token with Telegram, and this test only needs a non-iframe channel doc to exist.
        await using var app = await NewAppAsync();

        var token = Guid.NewGuid().ToString("N");
        const string channelId = "telegram-x";
        using (var session = app.Store.OpenAsyncSession(app.Slug))
        {
            await session.StoreAsync(new Channel
            {
                Id = $"channels/{channelId}",
                Type = ChannelType.Telegram,
                AgentId = "demo-agent",
                Enabled = true,
            });
            await session.StoreAsync(new EmbedLink
            {
                Id = $"embed-links/{token}",
                ChannelId = channelId,
                AgentId = "demo-agent",
                ExpiresAt = DateTime.UtcNow.AddHours(1),
                MaxInvocations = 10,
                CreatedAt = DateTime.UtcNow,
            });
            await session.SaveChangesAsync();
        }

        var pageEx = await Assert.ThrowsAsync<QuillHttpException>(() => app.GetEmbedPageAsync(token));
        Assert.Equal(HttpStatusCode.NotFound, pageEx.StatusCode);

        var chatEx = await Assert.ThrowsAsync<QuillHttpException>(() => app.SendEmbedChatAsync(token, "hi"));
        Assert.Equal(HttpStatusCode.NotFound, chatEx.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Embed_chat_returns_404_for_unregistered_agent()
    {
        // Seed the link + channel directly: no EP creates a channel pointing at a deleted agent.
        await using var app = await NewAppAsync();

        var token = Guid.NewGuid().ToString("N");
        const string channelId = "ghost-agent";
        using (var session = app.Store.OpenAsyncSession(app.Slug))
        {
            await session.StoreAsync(new Channel
            {
                Id = $"channels/{channelId}",
                Type = ChannelType.IFrame,
                AgentId = "ghost-agent",
                Enabled = true,
            });
            await session.StoreAsync(new EmbedLink
            {
                Id = $"embed-links/{token}",
                ChannelId = channelId,
                AgentId = "ghost-agent",
                ExpiresAt = DateTime.UtcNow.AddHours(1),
                MaxInvocations = 10,
                CreatedAt = DateTime.UtcNow,
            });
            await session.SaveChangesAsync();
        }

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => app.SendEmbedChatAsync(token, "hi"));
        Assert.Equal(HttpStatusCode.NotFound, ex.StatusCode);

        using (var session = app.Store.OpenAsyncSession(app.Slug))
        {
            var link = await session.LoadAsync<EmbedLink>($"embed-links/{token}");
            Assert.Equal(0, link.InvocationCount);
        }
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Channel_lifecycle_provision_update_delete_reprovision()
    {
        await using var app = await NewAppAsync();
        var channelId = await ProvisionIFrameChannelAsync(app);

        await app.UpdateChannelAsync(channelId, new UpdateChannelRequest(null, null, Enabled: false));

        await app.DeleteChannelAsync(channelId);

        var reChannel = await app.ProvisionChannelAsync(new ProvisionChannelRequest(ChannelType.IFrame, "demo-agent", new[] { "http://localhost" }));
        Assert.NotEqual(channelId, reChannel.ChannelId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task CdcProgress_relays_a_live_frame_over_websocket()
    {
        await using var app = await NewAppAsync();

        var wsClient = Host.Factory.Server.CreateWebSocketClient();
        wsClient.ConfigureRequest = request =>
            request.Headers[ApiKeyAuthenticationHandler.HeaderName] = ApplianceWebApplicationFactory.TestApiKey;
        var wsUri = new Uri(Host.Factory.Server.BaseAddress, $"api/apps/{app.Slug}/cdc/progress");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        using var ws = await wsClient.ConnectAsync(wsUri, cts.Token);

        // The proxied cdc-sink feed emits a ~4s heartbeat even with no CDC task, so a frame always arrives.
        var buffer = new byte[16 * 1024];
        var result = await ws.ReceiveAsync(new ArraySegment<byte>(buffer), cts.Token);
        Assert.NotEqual(WebSocketMessageType.Close, result.MessageType);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task CdcProgress_returns_404_for_unknown_slug()
    {
        // Plain GET (not a WS upgrade): the app-not-found check runs first → 404.
        var resp = await Host.Client.GetAsync(QuillRoutes.CdcProgress("nonexistent"));
        Assert.Equal(HttpStatusCode.NotFound, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task SetupTry_streams_ndjson_for_the_draft()
    {
        await using var app = await NewAppAsync();

        var csName = await SeedDemoAgentAsync(app);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var req = new HttpRequestMessage(HttpMethod.Post, QuillRoutes.SetupTry(app.Slug))
        {
            Content = JsonContent.Create(new
            {
                prompt = "hello",
                configuration = new
                {
                    name = "Draft Agent",
                    systemPrompt = "You are a placeholder draft agent.",
                    connectionStringName = csName,
                },
            }),
        };
        var resp = await Host.Client.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, cts.Token);
        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        Assert.Contains("application/x-ndjson", resp.Content.Headers.ContentType?.ToString() ?? "");

        var line = await ReadFirstLineAsync(resp, l => string.IsNullOrWhiteSpace(l) == false, cts.Token);
        Assert.NotNull(line);
        using var doc = JsonDocument.Parse(line!);
        var type = doc.RootElement.GetProperty("type").GetString();
        Assert.Contains(type, new[] { "chunk", "done", "error" });
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task SetupTry_returns_400_when_prompt_missing()
    {
        await using var app = await NewAppAsync();

        // raw: setup/try is a streaming endpoint — no typed happy wrapper to reuse
        var resp = await Host.Client.PostAsJsonAsync(QuillRoutes.SetupTry(app.Slug),
            new { configuration = new { name = "Draft", systemPrompt = "p", connectionStringName = "demo-llm" } });
        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task SetupTry_returns_400_when_configuration_missing()
    {
        await using var app = await NewAppAsync();

        // raw: setup/try is a streaming endpoint — no typed happy wrapper to reuse
        var resp = await Host.Client.PostAsJsonAsync(QuillRoutes.SetupTry(app.Slug), new { prompt = "hi" });
        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task SetupTry_returns_404_for_unknown_slug()
    {
        // raw: setup/try is a streaming endpoint — no typed happy wrapper to reuse
        var resp = await Host.Client.PostAsJsonAsync(QuillRoutes.SetupTry("nonexistent"),
            new { prompt = "hi", configuration = new { name = "Draft", systemPrompt = "p", connectionStringName = "demo-llm" } });
        Assert.Equal(HttpStatusCode.NotFound, resp.StatusCode);
    }

    private static async Task<string> ProvisionIFrameChannelAsync(QuillApp app, string agentId = "demo-agent")
    {
        await SeedDemoAgentAsync(app, agentId);
        var channel = await app.ProvisionChannelAsync(new ProvisionChannelRequest(ChannelType.IFrame, agentId, new[] { "http://localhost" }));
        return channel.ChannelId;
    }

    private static async Task<string> SeedDemoAgentAsync(QuillApp app, string agentId = "demo-agent")
    {
        await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = agentId,
            Name = "Demo Agent",
            SystemPrompt = "You are a placeholder demo agent.",
            ConnectionStringName = app.Host.ConnectionStringName,
        });
        return app.Host.ConnectionStringName;
    }

    private static async Task<string> MintLinkAsync(QuillApp app, string channelId)
    {
        var minted = await app.MintEmbedLinkAsync(new MintEmbedLinkRequest(channelId, [], 3600, 50));
        return minted.Token;
    }

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
        }

        return null;
    }
}
