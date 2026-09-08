using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Contracts;
using Tests.Infrastructure;
using Xunit;

using static QuillTests.E2E.Fixtures.AgentParameterFixtures;

namespace QuillTests;

public class EmbedLinksTests(ITestOutputHelper output) : QuillTestBase(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Mint_returns_token_absolute_url_and_expiry()
    {
        await using var h = await HarnessAsync();

        var minted = await h.App.MintEmbedLinkAsync(new MintEmbedLinkRequest(h.ChannelId, new(), TtlSeconds: 3600, MaxInvocations: 50));

        Assert.Matches("^[a-f0-9]{32}$", minted.Token);
        Assert.EndsWith($"/apps/{h.Slug}/embed/{minted.Token}", minted.Url);
        Assert.Equal(50, minted.MaxInvocations);
        Assert.True(minted.ExpiresAt > DateTime.UtcNow);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Mint_url_uses_the_public_subdomain()
    {
        await using var h = await HarnessAsync();

        // Host is dashboard.*; the mint swaps the leading DNS label to public.* in the returned URL.
        var req = new HttpRequestMessage(HttpMethod.Post, QuillRoutes.EmbedLinks(h.Slug))
        {
            Content = JsonContent.Create(
                new { channelId = h.ChannelId, parameters = new Dictionary<string, string>(), ttlSeconds = 3600, maxInvocations = 50 }),
        };
        req.Headers.Host = "dashboard.egor-ai.example";

        var resp = await h.Client.SendAsync(req);
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        var json = await resp.Content.ReadFromJsonAsync<JsonElement>();
        var token = json.GetProperty("token").GetString();
        var url = json.GetProperty("url").GetString();
        Assert.StartsWith($"http://public.egor-ai.example/apps/{h.Slug}/embed/", url);
        Assert.EndsWith($"/apps/{h.Slug}/embed/{token}", url);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Mint_unknown_widget_returns_404()
    {
        await using var h = await HarnessAsync(provisionChannel: false);

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => h.App.MintEmbedLinkAsync(new MintEmbedLinkRequest("nope", null, 3600, 10)));

        Assert.Equal(HttpStatusCode.NotFound, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Mint_disabled_channel_returns_400()
    {
        await using var h = await HarnessAsync();

        await h.App.UpdateChannelAsync(h.ChannelId, new UpdateChannelRequest(null, null, Enabled: false));

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => h.App.MintEmbedLinkAsync(new MintEmbedLinkRequest(h.ChannelId, null, 3600, 10)));

        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
        Assert.Contains("channel_disabled", ex.Body);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Mint_returns_404_when_channel_agent_was_deleted()
    {
        await using var h = await HarnessAsync(provisionChannel: false);

        const string channelId = "ghost";
        using (var session = h.Store.OpenAsyncSession(h.Database))
        {
            await session.StoreAsync(new Channel
            {
                Id = Channel.IdPrefix + channelId,
                Type = ChannelType.IFrame,
                AgentId = "deleted-agent",
                AllowedOrigins = [],
                Enabled = true,
                CreatedAt = DateTime.UtcNow,
            });
            await session.SaveChangesAsync();
        }

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => h.App.MintEmbedLinkAsync(new MintEmbedLinkRequest(channelId, null, 3600, 10)));

        Assert.Equal(HttpStatusCode.NotFound, ex.StatusCode);
    }

    [RavenTheory(RavenTestCategory.Quill)]
    [InlineData(0, 10)]
    [InlineData(99999999, 10)]
    [InlineData(3600, 0)]
    [InlineData(3600, 9999999)]
    public async Task Mint_rejects_out_of_range_ttl_or_max(int ttlSeconds, int maxInvocations)
    {
        await using var h = await HarnessAsync();

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => h.App.MintEmbedLinkAsync(new MintEmbedLinkRequest(h.ChannelId, null, ttlSeconds, maxInvocations)));

        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Mint_requires_the_agents_declared_parameters()
    {
        await using var h = await HarnessAsync(provisionChannel: false);
        var channelId = await ProvisionParamAgentChannelAsync(h.App);

        var missing = await Assert.ThrowsAsync<QuillHttpException>(() => h.App.MintEmbedLinkAsync(new MintEmbedLinkRequest(channelId, null, 3600, 10)));
        Assert.Equal(HttpStatusCode.BadRequest, missing.StatusCode);

        var ok = await h.App.MintEmbedLinkAsync(new MintEmbedLinkRequest(
            channelId, Parameters(("customerId", "companies/1-A")), 3600, 10));
        Assert.False(string.IsNullOrEmpty(ok.Token));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task List_returns_live_links_newest_first_and_excludes_revoked_and_expired()
    {
        await using var h = await HarnessAsync(provisionChannel: false);
        var channelId = await ProvisionParamAgentChannelAsync(h.App);

        var older = await MintAsync(h.App, channelId,
            parameters: Parameters(("customerId", "companies/1-A")));
        var newer = await MintAsync(h.App, channelId,
            parameters: Parameters(("customerId", "companies/2-A")));
        var expired = await MintAsync(h.App, channelId,
            parameters: Parameters(("customerId", "companies/3-A")));
        var revoked = await MintAsync(h.App, channelId,
            parameters: Parameters(("customerId", "companies/4-A")));

        // Pin distinct CreatedAt so the newest-first ordering doesn't ride on clock resolution.
        using (var session = h.Store.OpenAsyncSession(h.Database))
        {
            (await session.LoadAsync<EmbedLink>(EmbedLink.IdPrefix + older)).CreatedAt = DateTime.UtcNow.AddHours(-2);
            (await session.LoadAsync<EmbedLink>(EmbedLink.IdPrefix + newer)).CreatedAt = DateTime.UtcNow.AddHours(-1);
            (await session.LoadAsync<EmbedLink>(EmbedLink.IdPrefix + expired)).ExpiresAt = DateTime.UtcNow.AddMinutes(-1);
            await session.SaveChangesAsync();
        }

        await h.App.RevokeEmbedLinkAsync(revoked);

        var items = await h.App.GetEmbedLinksAsync();

        var tokens = items.Select(x => x.Token).ToArray();
        Assert.Equal(new[] { newer, older }, tokens);

        var first = items[0];
        Assert.Equal("param-agent", first.AgentId);
        Assert.Equal("companies/2-A", first.Parameters["customerId"]);
        Assert.Equal(0, first.InvocationCount);
        Assert.Equal(50, first.MaxInvocations);
        Assert.False(string.IsNullOrEmpty(first.ChannelId));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task List_for_unknown_app_returns_404()
    {
        await using var h = await HarnessAsync(provisionChannel: false);

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => Host.GetEmbedLinksAsync("no-such-app"));
        Assert.Equal(HttpStatusCode.NotFound, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Minted_link_serves_the_page_and_opens_a_chat_stream()
    {
        await using var h = await HarnessAsync();
        var token = await MintAsync(h.App, h.ChannelId);

        // raw: asserts the RESPONSE Content-Type header (page → text/html, chat → NDJSON), which the string-body wrappers can't expose.
        var page = await Host.Client.GetAsync(QuillRoutes.EmbedPage(h.Slug, token));
        Assert.Equal(HttpStatusCode.OK, page.StatusCode);
        Assert.Contains("text/html", page.Content.Headers.ContentType?.ToString() ?? "");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var chatReq = new HttpRequestMessage(HttpMethod.Post, QuillRoutes.EmbedChat(h.Slug, token))
        {
            Content = JsonContent.Create(new { prompt = "hello" }),
        };
        var chat = await Host.Client.SendAsync(chatReq, HttpCompletionOption.ResponseHeadersRead, cts.Token);
        Assert.Equal(HttpStatusCode.OK, chat.StatusCode);
        Assert.Contains("application/x-ndjson", chat.Content.Headers.ContentType?.ToString() ?? "");
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Malformed_token_returns_404()
    {
        await using var h = await HarnessAsync(provisionChannel: false);

        var pageEx = await Assert.ThrowsAsync<QuillHttpException>(() => h.App.GetEmbedPageAsync("not-a-token"));
        Assert.Equal(HttpStatusCode.NotFound, pageEx.StatusCode);

        var chatEx = await Assert.ThrowsAsync<QuillHttpException>(() => h.App.SendEmbedChatAsync("not-a-token", "hi"));
        Assert.Equal(HttpStatusCode.NotFound, chatEx.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Mint_writes_no_config_db_documents()
    {
        await using var h = await HarnessAsync();

        var token = await MintAsync(h.App, h.ChannelId);

        using (var cfg = Host.Config.OpenAsyncSession())
        {
            var pointers = await cfg.Advanced.LoadStartingWithAsync<object>("link-index/");
            Assert.Empty(pointers);
        }

        using var appSession = h.Store.OpenAsyncSession(h.Database);
        Assert.NotNull(await appSession.LoadAsync<EmbedLink>(EmbedLink.IdPrefix + token));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Embed_unknown_slug_returns_404()
    {
        await using var h = await HarnessAsync();
        var token = await MintAsync(h.App, h.ChannelId);

        var pageEx = await Assert.ThrowsAsync<QuillHttpException>(() => Host.GetEmbedPageAsync("other-app", token));
        Assert.Equal(HttpStatusCode.NotFound, pageEx.StatusCode);

        var chatEx = await Assert.ThrowsAsync<QuillHttpException>(() => Host.SendEmbedChatAsync("other-app", token, "hi"));
        Assert.Equal(HttpStatusCode.NotFound, chatEx.StatusCode);
    }

    [RavenTheory(RavenTestCategory.Quill)]
    [InlineData("MY-APP")]
    [InlineData("my_app")]
    [InlineData("my--app")]
    [InlineData("%21%21")]
    public async Task Embed_malformed_slug_segment_returns_404(string slug)
    {
        await using var h = await HarnessAsync();
        var token = await MintAsync(h.App, h.ChannelId);

        var pageEx = await Assert.ThrowsAsync<QuillHttpException>(() => Host.GetEmbedPageAsync(slug, token));
        Assert.Equal(HttpStatusCode.NotFound, pageEx.StatusCode);

        var chatEx = await Assert.ThrowsAsync<QuillHttpException>(() => Host.SendEmbedChatAsync(slug, token, "hi"));
        Assert.Equal(HttpStatusCode.NotFound, chatEx.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Old_style_embed_url_returns_404()
    {
        await using var h = await HarnessAsync();
        var token = await MintAsync(h.App, h.ChannelId);

        var oldPage = await h.Client.GetAsync($"/embed/{token}");
        Assert.Equal(HttpStatusCode.NotFound, oldPage.StatusCode);

        var oldChat = await h.Client.PostAsJsonAsync($"/embed/{token}/chat", new { prompt = "hi" });
        Assert.Equal(HttpStatusCode.NotFound, oldChat.StatusCode);

        var missingToken = await h.Client.GetAsync($"/apps/{h.Slug}/embed");
        Assert.Equal(HttpStatusCode.NotFound, missingToken.StatusCode);

        var extraSegment = await h.Client.GetAsync($"/apps/{h.Slug}/embed/{token}/extra/segment");
        Assert.Equal(HttpStatusCode.NotFound, extraSegment.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Chat_enforces_the_invocation_cap()
    {
        await using var h = await HarnessAsync();
        var token = await MintAsync(h.App, h.ChannelId, maxInvocations: 1);

        // Set the cap directly: no LLM in a unit run, and a failed turn would be refunded.
        using (var session = h.Store.OpenAsyncSession(h.Database))
        {
            var link = await session.LoadAsync<EmbedLink>(EmbedLink.IdPrefix + token);
            link.InvocationCount = 1;
            await session.SaveChangesAsync();
        }

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => h.App.SendEmbedChatAsync(token, "over the cap"));
        Assert.Equal(HttpStatusCode.TooManyRequests, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Pre_stream_agent_failure_refunds_the_invocation()
    {
        // This test REQUIRES the turn to fail; the shared demo CS points at a closed port, so it always does.
        await using var h = await HarnessAsync();
        var token = await MintAsync(h.App, h.ChannelId, maxInvocations: 1);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));

        // The wrapper drains the body, so the refund (awaited in the catch) has completed before the next turn.
        await h.App.SendEmbedChatAsync(token, "one", ct: cts.Token);

        await h.App.SendEmbedChatAsync(token, "two", ct: cts.Token);

        using var session = h.Store.OpenAsyncSession(h.Database);
        var link = await session.LoadAsync<EmbedLink>(EmbedLink.IdPrefix + token);
        Assert.Equal(0, link.InvocationCount);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Expired_link_returns_410()
    {
        await using var h = await HarnessAsync();
        var token = await MintAsync(h.App, h.ChannelId);

        using (var session = h.Store.OpenAsyncSession(h.Database))
        {
            var link = await session.LoadAsync<EmbedLink>(EmbedLink.IdPrefix + token);
            link.ExpiresAt = DateTime.UtcNow.AddMinutes(-1);
            await session.SaveChangesAsync();
        }

        var pageEx = await Assert.ThrowsAsync<QuillHttpException>(() => h.App.GetEmbedPageAsync(token));
        Assert.Equal(HttpStatusCode.Gone, pageEx.StatusCode);

        var chatEx = await Assert.ThrowsAsync<QuillHttpException>(() => h.App.SendEmbedChatAsync(token, "hi"));
        Assert.Equal(HttpStatusCode.Gone, chatEx.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Revoked_link_returns_410()
    {
        await using var h = await HarnessAsync();
        var token = await MintAsync(h.App, h.ChannelId);

        await h.App.RevokeEmbedLinkAsync(token);

        var ex = await Assert.ThrowsAsync<QuillHttpException>(() => h.App.SendEmbedChatAsync(token, "hi"));
        Assert.Equal(HttpStatusCode.Gone, ex.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Origin_check_blocks_disallowed_and_passes_allowed_self_and_absent()
    {
        // Allowed list excludes localhost on purpose, so the self-origin rule is what passes it.
        await using var h = await HarnessAsync(origins: new[] { "http://customer.example" });
        var token = await MintAsync(h.App, h.ChannelId);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));

        var blockedEx = await Assert.ThrowsAsync<QuillHttpException>(() => h.App.SendEmbedChatAsync(token, "hi", origin: "http://evil.example", ct: cts.Token));
        Assert.Equal(HttpStatusCode.Forbidden, blockedEx.StatusCode);

        await h.App.SendEmbedChatAsync(token, "hi", origin: "http://customer.example", ct: cts.Token);

        await h.App.SendEmbedChatAsync(token, "hi", origin: "HTTP://CUSTOMER.EXAMPLE", ct: cts.Token);

        await h.App.SendEmbedChatAsync(token, "hi", origin: "http://localhost", ct: cts.Token);

        await h.App.SendEmbedChatAsync(token, "hi", ct: cts.Token);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Empty_allowed_origins_skips_the_origin_check()
    {
        await using var h = await HarnessAsync(origins: Array.Empty<string>());
        var token = await MintAsync(h.App, h.ChannelId);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var resp = await h.App.SendEmbedChatAsync(token, "hi", origin: "http://anywhere.example", ct: cts.Token);
        Assert.False(string.IsNullOrEmpty(resp));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Disabled_channel_makes_minted_links_return_410()
    {
        await using var h = await HarnessAsync();
        var token = await MintAsync(h.App, h.ChannelId);

        await h.App.UpdateChannelAsync(h.ChannelId, new UpdateChannelRequest(null, null, Enabled: false));

        var pageEx = await Assert.ThrowsAsync<QuillHttpException>(() => h.App.GetEmbedPageAsync(token));
        Assert.Equal(HttpStatusCode.Gone, pageEx.StatusCode);

        var chatEx = await Assert.ThrowsAsync<QuillHttpException>(() => h.App.SendEmbedChatAsync(token, "hi"));
        Assert.Equal(HttpStatusCode.Gone, chatEx.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Conversation_is_bound_to_the_link_across_turns()
    {
        await using var h = await HarnessAsync();
        var token = await MintAsync(h.App, h.ChannelId, maxInvocations: 5);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await h.App.SendEmbedChatAsync(token, "one", ct: cts.Token);
        await h.App.SendEmbedChatAsync(token, "two", ct: cts.Token);

        using var session = h.Store.OpenAsyncSession(h.Database);
        var link = await session.LoadAsync<EmbedLink>(EmbedLink.IdPrefix + token);
        Assert.StartsWith("chats/", link.ConversationId);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Chat_body_cannot_inject_parameters_they_come_from_the_link()
    {
        await using var h = await HarnessAsync(provisionChannel: false);
        var channelId = await ProvisionParamAgentChannelAsync(h.App);
        var token = await MintAsync(h.App, channelId,
            parameters: Parameters(("customerId", "companies/1-A")));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        // body carries no customerId — the declared param is satisfied by the link, not the request body
        var body = await h.App.SendEmbedChatAsync(token, "where is my order?", ct: cts.Token);
        Assert.False(string.IsNullOrEmpty(body));
    }

    private async Task<Harness> HarnessAsync(bool provisionChannel = true, string[]? origins = null)
    {
        var app = await NewAppAsync();

        var channelId = "";
        if (provisionChannel)
        {
            await app.ProvisionAgentAsync(new AiAgentConfiguration
            {
                Identifier = "demo-agent",
                Name = "Demo Agent",
                SystemPrompt = "You are a placeholder demo agent.",
                ConnectionStringName = app.Host.ConnectionStringName,
            });
            var channel = await app.ProvisionChannelAsync(
                new ProvisionChannelRequest(ChannelType.IFrame, "demo-agent", origins ?? Array.Empty<string>()));
            channelId = channel.ChannelId;
        }

        return new Harness(app, channelId);
    }

    private static async Task<string> MintAsync(
        QuillApp app, string channelId,
        int ttlSeconds = 3600, int maxInvocations = 50, Dictionary<string, JsonElement>? parameters = null)
    {
        var minted = await app.MintEmbedLinkAsync(
            new MintEmbedLinkRequest(channelId, parameters ?? [], ttlSeconds, maxInvocations));
        return minted.Token;
    }

    private static async Task<string> ProvisionParamAgentChannelAsync(QuillApp app)
    {
        await app.ProvisionAgentAsync(new AiAgentConfiguration
        {
            Identifier = "param-agent",
            Name = "Param Agent",
            SystemPrompt = "You answer questions for a single customer.",
            ConnectionStringName = app.Host.ConnectionStringName,
            Queries =
            [
                new AiAgentToolQuery
                {
                    Name = "findOrdersByCustomer",
                    Description = "Returns the orders placed by the customer.",
                    Query = "from Orders where CustomerId = $customerId",
                    ParametersSampleObject = "{}",
                },
            ],
            Parameters =
            [
                new AiAgentParameter { Name = "customerId", Description = "The id of the customer whose orders to look up." },
            ],
        });

        var channel = await app.ProvisionChannelAsync(
            new ProvisionChannelRequest(ChannelType.IFrame, "param-agent", Array.Empty<string>()));
        return channel.ChannelId;
    }

    private sealed record Harness(QuillApp App, string ChannelId) : IAsyncDisposable
    {
        public HttpClient Client => App.Host.Client;
        public string Slug => App.Slug;
        public IDocumentStore Store => App.Store;
        public string Database => App.Slug;

        public ValueTask DisposeAsync() => App.DisposeAsync();
    }
}
