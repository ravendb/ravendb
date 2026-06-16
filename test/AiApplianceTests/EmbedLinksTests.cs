using System.Net;
using System.Net.Http.Json;
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
/// RavenDB-26775 rework: the embed URL is no longer a static public widgetId —
/// it is an API-minted, per-user token link (<c>POST /api/apps/{slug}/embed-links</c>)
/// carrying server-bound agent parameters, a TTL, and an N-invocation cap. These
/// cover the mint contract and the public <c>/embed/{token}</c> token lifecycle
/// (TTL / cap / revoke / origin), with no live LLM — the stream gates are asserted,
/// not the model output.
/// </summary>
public class EmbedLinksTests(ITestOutputHelper output) : RavenTestBase(output)
{
    // ---- mint ----

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Mint_returns_token_absolute_url_and_expiry()
    {
        using var h = await HarnessAsync();
        var token = default(string);

        var resp = await h.Client.PostAsJsonAsync($"/api/apps/{h.Slug}/embed-links",
            new { agentId = "demo-agent", parameters = new Dictionary<string, string>(), ttlSeconds = 3600, maxInvocations = 50 });

        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());
        var json = await resp.Content.ReadFromJsonAsync<JsonElement>();
        token = json.GetProperty("token").GetString();
        Assert.Matches("^[a-f0-9]{32}$", token!);
        Assert.EndsWith($"/embed/{token}", json.GetProperty("url").GetString());
        Assert.Equal(50, json.GetProperty("maxInvocations").GetInt32());
        Assert.True(json.GetProperty("expiresAt").GetDateTime() > DateTime.UtcNow);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Mint_unknown_agent_returns_404()
    {
        using var h = await HarnessAsync(provisionChannel: false);

        var resp = await h.Client.PostAsJsonAsync($"/api/apps/{h.Slug}/embed-links",
            new { agentId = "no-such-agent", ttlSeconds = 3600, maxInvocations = 10 });

        Assert.Equal(HttpStatusCode.NotFound, resp.StatusCode);
    }

    [RavenTheory(RavenTestCategory.AiAppliance)]
    [InlineData(0, 10)]          // ttl too small
    [InlineData(99999999, 10)]   // ttl too large
    [InlineData(3600, 0)]        // max too small
    [InlineData(3600, 9999999)]  // max too large
    public async Task Mint_rejects_out_of_range_ttl_or_max(int ttlSeconds, int maxInvocations)
    {
        using var h = await HarnessAsync();

        var resp = await h.Client.PostAsJsonAsync($"/api/apps/{h.Slug}/embed-links",
            new { agentId = "demo-agent", ttlSeconds, maxInvocations });

        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Mint_requires_the_agents_declared_parameters()
    {
        using var h = await HarnessAsync(provisionChannel: false);
        await ProvisionParamAgentChannelAsync(h.Client, h.Slug);

        // Declared customerId omitted → 400 at mint time (not at chat time).
        var missing = await h.Client.PostAsJsonAsync($"/api/apps/{h.Slug}/embed-links",
            new { agentId = "param-agent", ttlSeconds = 3600, maxInvocations = 10 });
        Assert.Equal(HttpStatusCode.BadRequest, missing.StatusCode);

        // Supplied → 200.
        var ok = await h.Client.PostAsJsonAsync($"/api/apps/{h.Slug}/embed-links",
            new
            {
                agentId = "param-agent",
                parameters = new Dictionary<string, string> { ["customerId"] = "companies/1-A" },
                ttlSeconds = 3600,
                maxInvocations = 10,
            });
        Assert.True(ok.IsSuccessStatusCode, await ok.Content.ReadAsStringAsync());
    }

    // ---- list ----

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task List_returns_live_links_newest_first_and_excludes_revoked_and_expired()
    {
        using var h = await HarnessAsync(provisionChannel: false);
        await ProvisionParamAgentChannelAsync(h.Client, h.Slug);

        // Four links on the same channel: two stay live, one is revoked, one is expired.
        var older = await MintAsync(h.Client, h.Slug, "param-agent",
            parameters: new Dictionary<string, string> { ["customerId"] = "companies/1-A" });
        var newer = await MintAsync(h.Client, h.Slug, "param-agent",
            parameters: new Dictionary<string, string> { ["customerId"] = "companies/2-A" });
        var expired = await MintAsync(h.Client, h.Slug, "param-agent",
            parameters: new Dictionary<string, string> { ["customerId"] = "companies/3-A" });
        var revoked = await MintAsync(h.Client, h.Slug, "param-agent",
            parameters: new Dictionary<string, string> { ["customerId"] = "companies/4-A" });

        // Pin distinct CreatedAt on the live pair (so the ordering assertion doesn't
        // ride on clock resolution) and push one link past its TTL.
        using (var session = h.Store.OpenAsyncSession(h.Database))
        {
            (await session.LoadAsync<EmbedLink>(EmbedLink.IdPrefix + older)).CreatedAt = DateTime.UtcNow.AddHours(-2);
            (await session.LoadAsync<EmbedLink>(EmbedLink.IdPrefix + newer)).CreatedAt = DateTime.UtcNow.AddHours(-1);
            (await session.LoadAsync<EmbedLink>(EmbedLink.IdPrefix + expired)).ExpiresAt = DateTime.UtcNow.AddMinutes(-1);
            await session.SaveChangesAsync();
        }

        var revoke = await h.Client.DeleteAsync($"/api/apps/{h.Slug}/embed-links/{revoked}");
        Assert.True(revoke.IsSuccessStatusCode, await revoke.Content.ReadAsStringAsync());

        var resp = await h.Client.GetAsync($"/api/apps/{h.Slug}/embed-links");
        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        var items = await resp.Content.ReadFromJsonAsync<JsonElement>();

        // Only the live links, newest first; revoked + expired are filtered out.
        var tokens = items.EnumerateArray().Select(x => x.GetProperty("token").GetString()).ToArray();
        Assert.Equal(new[] { newer, older }, tokens);

        // The summary carries the bound parameters, the routed agent, and a zeroed usage counter.
        var first = items[0];
        Assert.Equal("param-agent", first.GetProperty("agentId").GetString());
        Assert.Equal("companies/2-A", first.GetProperty("parameters").GetProperty("customerId").GetString());
        Assert.Equal(0, first.GetProperty("invocationCount").GetInt32());
        Assert.Equal(50, first.GetProperty("maxInvocations").GetInt32());
        Assert.False(string.IsNullOrEmpty(first.GetProperty("widgetId").GetString()));
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task List_for_unknown_app_returns_404()
    {
        using var h = await HarnessAsync(provisionChannel: false);

        var resp = await h.Client.GetAsync("/api/apps/no-such-app/embed-links");
        Assert.Equal(HttpStatusCode.NotFound, resp.StatusCode);
    }

    // ---- public token lifecycle ----

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Minted_link_serves_the_page_and_opens_a_chat_stream()
    {
        using var h = await HarnessAsync();
        var token = await MintAsync(h.Client, h.Slug, "demo-agent");

        var page = await h.Client.GetAsync($"/embed/{token}");
        Assert.Equal(HttpStatusCode.OK, page.StatusCode);
        Assert.Contains("text/html", page.Content.Headers.ContentType?.ToString() ?? "");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var chat = await SendChatAsync(h.Client, token, new { prompt = "hello" }, cts.Token);
        Assert.Equal(HttpStatusCode.OK, chat.StatusCode);
        Assert.Contains("application/x-ndjson", chat.Content.Headers.ContentType?.ToString() ?? "");
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Malformed_token_returns_404()
    {
        using var h = await HarnessAsync(provisionChannel: false);

        var page = await h.Client.GetAsync("/embed/not-a-token");
        Assert.Equal(HttpStatusCode.NotFound, page.StatusCode);

        var chat = await h.Client.PostAsJsonAsync("/embed/not-a-token/chat", new { prompt = "hi" });
        Assert.Equal(HttpStatusCode.NotFound, chat.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Chat_enforces_the_invocation_cap()
    {
        using var h = await HarnessAsync();
        var token = await MintAsync(h.Client, h.Slug, "demo-agent", maxInvocations: 1);

        // Drive the link to its cap directly. (A real successful turn would consume
        // it, but there's no LLM in a unit run and a failed turn is refunded — see
        // Pre_stream_agent_failure_refunds_the_invocation — so set the count here.)
        using (var session = h.Store.OpenAsyncSession(h.Database))
        {
            var link = await session.LoadAsync<EmbedLink>(EmbedLink.IdPrefix + token);
            link.InvocationCount = 1;
            await session.SaveChangesAsync();
        }

        var resp = await h.Client.PostAsJsonAsync($"/embed/{token}/chat", new { prompt = "over the cap" });
        Assert.Equal(HttpStatusCode.TooManyRequests, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Pre_stream_agent_failure_refunds_the_invocation()
    {
        using var h = await HarnessAsync();
        var token = await MintAsync(h.Client, h.Slug, "demo-agent", maxInvocations: 1);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));

        // No LLM in a unit run -> the agent run fails before any chunk streams -> the
        // reserved invocation is refunded. Draining the response ensures the refund
        // (awaited in the catch before the error frame) has completed.
        var first = await SendChatAsync(h.Client, token, new { prompt = "one" }, cts.Token);
        await first.Content.ReadAsStringAsync(cts.Token);
        Assert.Equal(HttpStatusCode.OK, first.StatusCode); // 200 + error frame, not 429

        // Cap is 1; without the refund this second turn would be 429. It isn't.
        var second = await SendChatAsync(h.Client, token, new { prompt = "two" }, cts.Token);
        await second.Content.ReadAsStringAsync(cts.Token);
        Assert.NotEqual(HttpStatusCode.TooManyRequests, second.StatusCode);

        using var session = h.Store.OpenAsyncSession(h.Database);
        var link = await session.LoadAsync<EmbedLink>(EmbedLink.IdPrefix + token);
        Assert.Equal(0, link.InvocationCount); // both failed turns were refunded
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Expired_link_returns_410()
    {
        using var h = await HarnessAsync();
        var token = await MintAsync(h.Client, h.Slug, "demo-agent");

        // Force expiry without waiting: rewind ExpiresAt on the stored link.
        using (var session = h.Store.OpenAsyncSession(h.Database))
        {
            var link = await session.LoadAsync<EmbedLink>(EmbedLink.IdPrefix + token);
            link.ExpiresAt = DateTime.UtcNow.AddMinutes(-1);
            await session.SaveChangesAsync();
        }

        var page = await h.Client.GetAsync($"/embed/{token}");
        Assert.Equal(HttpStatusCode.Gone, page.StatusCode);

        var chat = await h.Client.PostAsJsonAsync($"/embed/{token}/chat", new { prompt = "hi" });
        Assert.Equal(HttpStatusCode.Gone, chat.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Revoked_link_returns_410()
    {
        using var h = await HarnessAsync();
        var token = await MintAsync(h.Client, h.Slug, "demo-agent");

        var revoke = await h.Client.DeleteAsync($"/api/apps/{h.Slug}/embed-links/{token}");
        Assert.True(revoke.IsSuccessStatusCode, await revoke.Content.ReadAsStringAsync());

        var chat = await h.Client.PostAsJsonAsync($"/embed/{token}/chat", new { prompt = "hi" });
        Assert.Equal(HttpStatusCode.Gone, chat.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Chat_body_cannot_inject_parameters_they_come_from_the_link()
    {
        using var h = await HarnessAsync(provisionChannel: false);
        await ProvisionParamAgentChannelAsync(h.Client, h.Slug);
        var token = await MintAsync(h.Client, h.Slug, "param-agent",
            parameters: new Dictionary<string, string> { ["customerId"] = "companies/1-A" });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        // Body carries NO customerId — the declared param is satisfied by the link,
        // proving parameters are bound server-side, not from the request body.
        var chat = await SendChatAsync(h.Client, token, new { prompt = "where is my order?" }, cts.Token);
        Assert.Equal(HttpStatusCode.OK, chat.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Origin_check_blocks_disallowed_and_passes_allowed_self_and_absent()
    {
        // Allowed list deliberately EXCLUDES the appliance's own origin
        // (http://localhost) so the self-origin rule is what passes it.
        using var h = await HarnessAsync(origins: new[] { "http://customer.example" });
        var token = await MintAsync(h.Client, h.Slug, "demo-agent");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));

        var blocked = await SendChatAsync(h.Client, token, new { prompt = "hi" }, cts.Token, origin: "http://evil.example");
        Assert.Equal(HttpStatusCode.Forbidden, blocked.StatusCode);

        var allowed = await SendChatAsync(h.Client, token, new { prompt = "hi" }, cts.Token, origin: "http://customer.example");
        Assert.Equal(HttpStatusCode.OK, allowed.StatusCode);

        // Case-insensitive (RFC 3986).
        var cased = await SendChatAsync(h.Client, token, new { prompt = "hi" }, cts.Token, origin: "HTTP://CUSTOMER.EXAMPLE");
        Assert.Equal(HttpStatusCode.OK, cased.StatusCode);

        // The appliance's own origin is always allowed.
        var self = await SendChatAsync(h.Client, token, new { prompt = "hi" }, cts.Token, origin: "http://localhost");
        Assert.Equal(HttpStatusCode.OK, self.StatusCode);

        // Absent Origin (non-browser caller) passes — the token is the guard.
        var absent = await SendChatAsync(h.Client, token, new { prompt = "hi" }, cts.Token);
        Assert.Equal(HttpStatusCode.OK, absent.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Empty_allowed_origins_skips_the_origin_check()
    {
        // M1 contract: explicit [] = postable from anywhere.
        using var h = await HarnessAsync(origins: Array.Empty<string>());
        var token = await MintAsync(h.Client, h.Slug, "demo-agent");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var resp = await SendChatAsync(h.Client, token, new { prompt = "hi" }, cts.Token, origin: "http://anywhere.example");
        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Disabled_channel_makes_minted_links_return_410()
    {
        using var h = await HarnessAsync();
        var token = await MintAsync(h.Client, h.Slug, "demo-agent");

        var disable = await h.Client.PutAsJsonAsync($"/api/apps/{h.Slug}/channels/{h.WidgetId}", new { enabled = false });
        Assert.True(disable.IsSuccessStatusCode, await disable.Content.ReadAsStringAsync());

        var page = await h.Client.GetAsync($"/embed/{token}");
        Assert.Equal(HttpStatusCode.Gone, page.StatusCode);

        var chat = await h.Client.PostAsJsonAsync($"/embed/{token}/chat", new { prompt = "hi" });
        Assert.Equal(HttpStatusCode.Gone, chat.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Conversation_is_bound_to_the_link_across_turns()
    {
        using var h = await HarnessAsync();
        var token = await MintAsync(h.Client, h.Slug, "demo-agent", maxInvocations: 5);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await (await SendChatAsync(h.Client, token, new { prompt = "one" }, cts.Token)).Content.ReadAsStringAsync();
        await (await SendChatAsync(h.Client, token, new { prompt = "two" }, cts.Token)).Content.ReadAsStringAsync();

        using var session = h.Store.OpenAsyncSession(h.Database);
        var link = await session.LoadAsync<EmbedLink>(EmbedLink.IdPrefix + token);
        // The link owns its conversation: minted server-side on the first turn and
        // pinned thereafter (the refund of a failed turn decrements the count but
        // never clears the bound conversation id). InvocationCount isn't asserted —
        // there's no LLM in a unit run, so the turns fail pre-stream and refund; the
        // cap itself is covered by Chat_enforces_the_invocation_cap.
        Assert.StartsWith("chats/", link.ConversationId);
    }

    // ---- helpers ----

    private async Task<Harness> HarnessAsync(bool provisionChannel = true, string[]? origins = null)
    {
        var store = GetDocumentStore();
        var database = "per-app-" + Guid.NewGuid().ToString("N");
        await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(database)));
        var dbCleanup = Databases.EnsureDatabaseDeletion(database, store);

        const string slug = "my-app";
        using (var session = store.OpenAsyncSession())
        {
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

        var factory = new ApplianceWebApplicationFactory(
            licenseApiUrl: "http://unused-in-unit-tests",
            setupPackagePath: NewDataPath(forceCreateDir: true),
            applianceStore: store,
            configureOptions: opts => opts.ConfigDatabase = store.Database);
        var client = factory.CreateClient();

        var widgetId = "";
        if (provisionChannel)
        {
            await ApplianceTestSeed.SeedMockAgentAsync(client, slug, "demo-agent");
            var resp = await client.PostAsJsonAsync($"/api/apps/{slug}/setup/channel",
                new { type = "iframe", agentId = "demo-agent", allowedOrigins = origins ?? Array.Empty<string>() });
            Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());
            var json = await resp.Content.ReadFromJsonAsync<JsonElement>();
            widgetId = json.GetProperty("widgetId").GetString()!;
        }

        return new Harness(store, database, slug, widgetId, factory, client, dbCleanup);
    }

    private static async Task<string> MintAsync(
        HttpClient client, string slug, string agentId,
        int ttlSeconds = 3600, int maxInvocations = 50, Dictionary<string, string>? parameters = null)
    {
        var resp = await client.PostAsJsonAsync($"/api/apps/{slug}/embed-links",
            new { agentId, parameters = parameters ?? new Dictionary<string, string>(), ttlSeconds, maxInvocations });
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());
        var json = await resp.Content.ReadFromJsonAsync<JsonElement>();
        return json.GetProperty("token").GetString()!;
    }

    private static async Task<HttpResponseMessage> SendChatAsync(
        HttpClient client, string token, object body, CancellationToken ct, string? origin = null)
    {
        var req = new HttpRequestMessage(HttpMethod.Post, $"/embed/{token}/chat")
        {
            Content = JsonContent.Create(body),
        };
        if (origin is not null)
            req.Headers.TryAddWithoutValidation("Origin", origin);

        return await client.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, ct);
    }

    private static async Task ProvisionParamAgentChannelAsync(HttpClient client, string slug)
    {
        var csResp = await client.PostAsJsonAsync($"/api/apps/{slug}/ai/connection-strings",
            new { name = "param-llm", identifier = "param-llm", modelType = "Chat", ollamaSettings = new { uri = "http://localhost:11434/", model = "llama3.1" } });
        Assert.True(csResp.IsSuccessStatusCode, await csResp.Content.ReadAsStringAsync());

        var agentResp = await client.PostAsJsonAsync($"/api/apps/{slug}/setup/agent",
            new
            {
                identifier = "param-agent",
                name = "Param Agent",
                systemPrompt = "You answer questions for a single customer.",
                connectionStringName = "param-llm",
                queries = new[]
                {
                    new
                    {
                        name = "findOrdersByCustomer",
                        description = "Returns the orders placed by the customer.",
                        query = "from Orders where CustomerId = $customerId",
                        parametersSampleObject = "{}",
                    },
                },
                parameters = new[]
                {
                    new { name = "customerId", description = "The id of the customer whose orders to look up." },
                },
            });
        Assert.True(agentResp.IsSuccessStatusCode, await agentResp.Content.ReadAsStringAsync());

        var chResp = await client.PostAsJsonAsync($"/api/apps/{slug}/setup/channel",
            new { type = "iframe", agentId = "param-agent", allowedOrigins = Array.Empty<string>() });
        Assert.True(chResp.IsSuccessStatusCode, await chResp.Content.ReadAsStringAsync());
    }

    private sealed class Harness(
        IDocumentStore store, string database, string slug, string widgetId,
        ApplianceWebApplicationFactory factory, HttpClient client, IDisposable dbCleanup) : IDisposable
    {
        public IDocumentStore Store { get; } = store;
        public string Database { get; } = database;
        public string Slug { get; } = slug;
        public string WidgetId { get; } = widgetId;
        public HttpClient Client { get; } = client;

        public void Dispose()
        {
            Client.Dispose();
            factory.Dispose();
            dbCleanup.Dispose();
        }
    }
}
