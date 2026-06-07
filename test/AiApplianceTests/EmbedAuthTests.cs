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
/// RavenDB-26700 (auth follow-up) — the embed chat token contract that closes
/// ayende's A2 (client-supplied conversationId over enumerable sequential
/// ids): turn 1 mints an opaque <c>cnv_</c> token (its own
/// <c>conversation</c> NDJSON frame, before the agent runs), continuation
/// requires it, the real <c>chats/</c> id never crosses the wire, and the
/// M1b Origin defense-in-depth gate 403s disallowed browser origins.
/// No live LLM needed: the wiring-level frames (conversation/error) carry
/// every assertion; the reply path itself is E2E coverage.
/// </summary>
public class EmbedAuthTests(ITestOutputHelper output) : RavenTestBase(output)
{
    // ---- turn 1: mint ----

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task First_turn_mints_a_conversation_token_and_hides_the_chat_id()
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

        var lines = await ReadAllLinesAsync(resp, cts.Token);
        Assert.NotEmpty(lines);

        // The minted token is the FIRST frame — emitted after the binding is
        // written and before the agent runs, so the client keeps the
        // conversation even when the reply itself fails (no-LLM unit runs).
        using var first = JsonDocument.Parse(lines[0]);
        Assert.Equal("conversation", first.RootElement.GetProperty("type").GetString());
        var token = first.RootElement.GetProperty("conversationToken").GetString();
        Assert.NotNull(token);
        Assert.StartsWith("cnv_", token, StringComparison.Ordinal);
        Assert.Equal("cnv_".Length + RandomIds.SuffixLength, token!.Length);

        // A2: the real conversation id never crosses the wire — in any frame.
        Assert.All(lines, line =>
        {
            Assert.DoesNotContain("chats/", line);
            Assert.DoesNotContain("conversationId", line);
        });

        // The binding doc holds the hidden chats/ id, keyed under this widget.
        using var session = store.OpenAsyncSession(perAppDb);
        var binding = await session.LoadAsync<ConversationBinding>(ConversationBinding.MakeId(widgetId, token));
        Assert.NotNull(binding);
        Assert.StartsWith("chats/", binding.ConversationId, StringComparison.Ordinal);
        Assert.Equal(widgetId, binding.WidgetId);
    }

    // ---- turn 2: resume ----

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Second_turn_resumes_with_the_token_instead_of_minting()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();
        var widgetId = await ProvisionIFrameChannelAsync(client, "my-app");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        var token = await MintTokenAsync(client, widgetId, cts.Token);

        var resp = await SendChatAsync(client, widgetId, new { prompt = "again", conversationToken = token }, cts.Token);
        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);

        // No re-mint: the stream opens straight into the run (chunk/done/error
        // wiring frame), never a second "conversation" frame...
        var lines = await ReadAllLinesAsync(resp, cts.Token);
        Assert.NotEmpty(lines);
        using var first = JsonDocument.Parse(lines[0]);
        Assert.Contains(first.RootElement.GetProperty("type").GetString(), new[] { "chunk", "done", "error" });

        // ...and no second binding doc either — the token resolved the
        // existing conversation.
        using var session = store.OpenAsyncSession(perAppDb);
        var bindings = await session.Advanced.LoadStartingWithAsync<ConversationBinding>(ConversationBinding.IdPrefix);
        Assert.Single(bindings);
    }

    // ---- 401 matrix ----

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Unknown_token_returns_401_conversation_unknown()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();
        var widgetId = await ProvisionIFrameChannelAsync(client, "my-app");

        // Well-formed but never minted — the binding load misses.
        var resp = await client.PostAsJsonAsync($"/embed/{widgetId}/chat",
            new { prompt = "hi", conversationToken = RandomIds.NewId("cnv_") });

        Assert.Equal(HttpStatusCode.Unauthorized, resp.StatusCode);
        Assert.Equal("conversation_unknown", await ReadErrorCodeAsync(resp));
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Expired_token_returns_401_conversation_expired()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();
        var widgetId = await ProvisionIFrameChannelAsync(client, "my-app");

        var token = RandomIds.NewId("cnv_");
        using (var session = store.OpenAsyncSession(perAppDb))
        {
            await session.StoreAsync(new ConversationBinding
            {
                Id = ConversationBinding.MakeId(widgetId, token),
                ConversationId = "chats/stale",
                WidgetId = widgetId,
                CreatedAt = DateTime.UtcNow.AddDays(-2),
                ExpiresAt = DateTime.UtcNow.AddDays(-1),
            });
            await session.SaveChangesAsync();
        }

        var resp = await client.PostAsJsonAsync($"/embed/{widgetId}/chat",
            new { prompt = "hi", conversationToken = token });

        Assert.Equal(HttpStatusCode.Unauthorized, resp.StatusCode);
        Assert.Equal("conversation_expired", await ReadErrorCodeAsync(resp));
    }

    [RavenTheory(RavenTestCategory.AiAppliance)]
    [InlineData("x")]                                   // no prefix, junk
    [InlineData("cnv_short")]                           // right prefix, wrong length
    [InlineData("cnv_aaaaaaaaaaaaaaaaaaaaaaa")]         // 23 chars — one too long
    [InlineData("chats/00000000000000000001-A")]        // a raw conversation id is NOT a token
    [InlineData("cnv_!!invalid@@chars##aaaa")]          // non-base64url chars
    public async Task Malformed_token_returns_401_without_a_doc_load(string token)
    {
        // L3: structural breakage only — no case-mangling variant here, doc-id
        // lookups are case-insensitive so a case-mangled valid token resolves.
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();
        var widgetId = await ProvisionIFrameChannelAsync(client, "my-app");

        var resp = await client.PostAsJsonAsync($"/embed/{widgetId}/chat",
            new { prompt = "hi", conversationToken = token });

        Assert.Equal(HttpStatusCode.Unauthorized, resp.StatusCode);
        Assert.Equal("conversation_unknown", await ReadErrorCodeAsync(resp));
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Token_minted_under_one_widget_is_rejected_by_another()
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
        var widgetA = await ProvisionIFrameChannelAsync(client, "app-a");
        var widgetB = await ProvisionIFrameChannelAsync(client, "app-b");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        var tokenA = await MintTokenAsync(client, widgetA, cts.Token);

        // The widgetId is part of the binding doc id — widget B's lookup is a
        // structural miss, no field comparison involved.
        var resp = await client.PostAsJsonAsync($"/embed/{widgetB}/chat",
            new { prompt = "hi", conversationToken = tokenA });

        Assert.Equal(HttpStatusCode.Unauthorized, resp.StatusCode);
        Assert.Equal("conversation_unknown", await ReadErrorCodeAsync(resp));
    }

    // ---- A2 regression ----

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Legacy_conversationId_field_is_ignored_and_cannot_reach_a_guessed_chat()
    {
        // THE A2 regression test: the pre-token contract let a visitor continue
        // any sequential chats/ id. The field no longer exists on the contract;
        // a body that still carries it must behave exactly like a fresh turn 1
        // and never touch the guessed conversation.
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();
        var widgetId = await ProvisionIFrameChannelAsync(client, "my-app");

        const string guessedId = "chats/00000000000000000001-A";
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var resp = await SendChatAsync(client, widgetId,
            new { prompt = "what did we talk about?", conversationId = guessedId }, cts.Token);

        // Unknown JSON members are ignored -> this is a token-less turn 1.
        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        var lines = await ReadAllLinesAsync(resp, cts.Token);
        Assert.NotEmpty(lines);
        using var first = JsonDocument.Parse(lines[0]);
        Assert.Equal("conversation", first.RootElement.GetProperty("type").GetString());

        // The fresh binding hides a fresh random id — never the guessed one.
        var token = first.RootElement.GetProperty("conversationToken").GetString();
        Assert.NotNull(token);
        using var session = store.OpenAsyncSession(perAppDb);
        var binding = await session.LoadAsync<ConversationBinding>(ConversationBinding.MakeId(widgetId, token));
        Assert.NotNull(binding);
        Assert.NotEqual(guessedId, binding.ConversationId);
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

        // C1 (Copilot review, PR #12): scheme/host are case-insensitive per
        // RFC 3986, and case can never distinguish two origins — so unusual
        // casing from a non-browser client must not 403 an allowed origin
        // (and the loosened compare cannot false-allow).
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
    public async Task Unknown_widget_with_token_returns_404_not_401()
    {
        // I2 (impl review 2026-06-07): resolve-before-auth is a contract, not
        // an accident of statement order — answering 401 for an unknown widget
        // would tell a prober holding any junk token which widgetIds exist.
        var store = GetDocumentStore();
        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.PostAsJsonAsync("/embed/wgt_nope/chat",
            new { prompt = "hi", conversationToken = RandomIds.NewId("cnv_") });

        Assert.Equal(HttpStatusCode.NotFound, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Disabled_channel_returns_410_before_any_auth_gate()
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

        // Garbage token + disabled channel -> the resolve gate answers first.
        var resp = await client.PostAsJsonAsync($"/embed/{widgetId}/chat",
            new { prompt = "hi", conversationToken = "garbage" });
        Assert.Equal(HttpStatusCode.Gone, resp.StatusCode);
    }

    // ---- helpers ----

    /// <summary>Runs a token-less turn 1 and returns the minted token from the
    /// leading <c>conversation</c> frame.</summary>
    private static async Task<string> MintTokenAsync(HttpClient client, string widgetId, CancellationToken ct)
    {
        var resp = await SendChatAsync(client, widgetId, new { prompt = "hello" }, ct);
        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
        var lines = await ReadAllLinesAsync(resp, ct);
        Assert.NotEmpty(lines);
        using var first = JsonDocument.Parse(lines[0]);
        Assert.Equal("conversation", first.RootElement.GetProperty("type").GetString());
        return first.RootElement.GetProperty("conversationToken").GetString()!;
    }

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
}
