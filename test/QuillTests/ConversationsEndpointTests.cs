using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Raven.Quill.Channels;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

/// <summary>
/// Coverage for <c>GET /api/apps/{slug}/conversations</c> (list) and
/// <c>/conversations/{*id}</c> (detail) — backs the prototype's
/// <c>listConversations</c> / <c>getConversation</c>. Shaped from <c>@conversations</c>
/// docs: agentName, derived agentInitials + state, last-exchange preview, and the
/// full chronological transcript on detail.
/// </summary>
public class ConversationsEndpointTests(ITestOutputHelper output) : ApplianceMetricsTestBase(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Conversations_list_and_detail_shape_transcript_state_and_agent()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        var now = DateTime.UtcNow;
        await SeedConversationAsync(store, perAppDb, "chats/recent", "order-support", now.AddMinutes(-10),
            turns: [("user", "hello"), ("assistant", "hi there")]);
        await SeedConversationAsync(store, perAppDb, "chats/old", "billing", now.AddDays(-3),
            turns: [("user", "where is my refund")]);

        // An iframe channel + embed link attributing chats/recent to it.
        await SeedChannelAsync(store, perAppDb, channelId: "wgt1", enabled: true);
        using (var session = store.OpenAsyncSession(perAppDb))
        {
            await session.StoreAsync(new EmbedLink
            {
                WidgetId = "wgt1",
                AgentId = "order-support",
                ExpiresAt = now.AddHours(1),
                MaxInvocations = 5,
                ConversationId = "chats/recent",
                CreatedAt = now.AddMinutes(-10),
            }, $"{EmbedLink.IdPrefix}{Guid.NewGuid():N}");
            await session.SaveChangesAsync();
        }

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        // List — newest first, with a last-exchange preview (full transcript stays detail-only),
        // derived state/initials, channel attribution.
        var list = await client.GetFromJsonAsync<JsonElement>("/api/apps/my-app/conversations");
        Assert.Equal(2, list.GetArrayLength());

        var first = list[0];
        Assert.Equal("chats/recent", first.GetProperty("id").GetString());
        Assert.Equal("my-app", first.GetProperty("appId").GetString());
        Assert.Equal("order-support", first.GetProperty("agentName").GetString());
        Assert.Equal("OS", first.GetProperty("agentInitials").GetString());
        Assert.Equal("active", first.GetProperty("state").GetString());          // 10 min ago
        var firstExchange = first.GetProperty("lastExchange");                   // last-exchange preview, newest first
        Assert.Equal(2, firstExchange.GetArrayLength());
        Assert.Equal("agent", firstExchange[0].GetProperty("role").GetString());
        Assert.Equal("hi there", firstExchange[0].GetProperty("text").GetString());
        Assert.Equal("user", firstExchange[1].GetProperty("role").GetString());
        Assert.Equal("hello", firstExchange[1].GetProperty("text").GetString());
        Assert.Equal(JsonValueKind.Null, first.GetProperty("transcript").ValueKind);
        Assert.Equal("wgt1", first.GetProperty("channelName").GetString());      // attributed via EmbedLink

        Assert.Equal("closed", list[1].GetProperty("state").GetString());        // 3 days ago
        Assert.Equal("", list[1].GetProperty("channelName").GetString());        // no embed link → unattributed

        // Detail — full transcript, chronological.
        var detail = await client.GetFromJsonAsync<JsonElement>("/api/apps/my-app/conversations/chats/recent");
        var transcript = detail.GetProperty("transcript");
        Assert.Equal(2, transcript.GetArrayLength());
        Assert.Equal("user", transcript[0].GetProperty("role").GetString());
        Assert.Equal("hello", transcript[0].GetProperty("text").GetString());

        // I1: outbound timestamps are UTC, ISO-8601 with a trailing Z (so the browser parses as UTC).
        Assert.EndsWith("Z\"", detail.GetProperty("lastActivityAt").GetRawText());
        Assert.EndsWith("Z\"", detail.GetProperty("startedAt").GetRawText());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Conversations_list_preview_returns_the_newest_two_turns_of_a_long_conversation()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        // More turns than the list preview's page size (LastExchangePageSize = 10), so the read must
        // fetch the most-recent tail — not the oldest page — otherwise TakeLast(2) surfaces the wrong
        // turns. m1..m14 alternate user/assistant; the newest exchange is m13 (user) + m14 (assistant).
        var turns = new (string Role, string Text)[14];
        for (var i = 0; i < turns.Length; i++)
            turns[i] = (i % 2 == 0 ? "user" : "assistant", $"m{i + 1}");
        await SeedConversationAsync(store, perAppDb, "chats/long", "agent-x", DateTime.UtcNow.AddMinutes(-5), turns: turns);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var list = await client.GetFromJsonAsync<JsonElement>("/api/apps/my-app/conversations");
        Assert.Equal(1, list.GetArrayLength());

        // Newest two turns, newest-first — proves the bounded page returned the tail, not the head.
        var exchange = list[0].GetProperty("lastExchange");
        Assert.Equal(2, exchange.GetArrayLength());
        Assert.Equal("agent", exchange[0].GetProperty("role").GetString());
        Assert.Equal("m14", exchange[0].GetProperty("text").GetString());
        Assert.Equal("user", exchange[1].GetProperty("role").GetString());
        Assert.Equal("m13", exchange[1].GetProperty("text").GetString());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task GetConversation_resolves_a_percent_encoded_slash_in_the_id()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);
        await SeedConversationAsync(store, perAppDb, "chats/recent", "order-support", DateTime.UtcNow.AddMinutes(-5),
            turns: [("user", "hello"), ("assistant", "hi there")]);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        // The browser client percent-encodes the document-id slash (chats/recent -> chats%2Frecent);
        // the endpoint must still resolve it to the real conversation.
        var detail = await client.GetFromJsonAsync<JsonElement>("/api/apps/my-app/conversations/chats%2Frecent");
        var transcript = detail.GetProperty("transcript");
        Assert.Equal(2, transcript.GetArrayLength());
        Assert.Equal("hello", transcript[0].GetProperty("text").GetString());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task GetConversation_returns_404_for_non_conversation_or_unknown_id()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);
        // A real, non-conversation doc in the per-app DB (channels/wgt1).
        await SeedChannelAsync(store, perAppDb, channelId: "wgt1", enabled: true);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        // M5: an id without the chats/ prefix must not load some other doc and shape it
        // as a conversation — it must 404.
        var nonConversation = await client.GetAsync("/api/apps/my-app/conversations/channels/wgt1");
        Assert.Equal(HttpStatusCode.NotFound, nonConversation.StatusCode);

        // An unknown chats/ id also 404s.
        var unknown = await client.GetAsync("/api/apps/my-app/conversations/chats/does-not-exist");
        Assert.Equal(HttpStatusCode.NotFound, unknown.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Conversations_list_loads_all_pages()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        // One past a single 1024-doc LoadStartingWith page — the list must page to return them all.
        const int count = 1025;
        var now = DateTime.UtcNow;
        await using (var bulk = store.BulkInsert(perAppDb))
            for (var i = 0; i < count; i++)
                await bulk.StoreAsync(new { Agent = "demo", CreatedAt = now, LastMessageAt = now }, $"chats/{i:D5}");

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var list = await client.GetFromJsonAsync<JsonElement>("/api/apps/my-app/conversations");
        Assert.Equal(count, list.GetArrayLength());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Conversation_detail_filters_scaffolding_and_extracts_array_content()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        // Real-shaped doc: system prompt + user + assistant(array content) + tool message.
        await SeedRealisticConversationAsync(store, perAppDb, "chats/real", "order-support", DateTime.UtcNow.AddMinutes(-5));

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var detail = await client.GetFromJsonAsync<JsonElement>("/api/apps/my-app/conversations/chats/real");
        var transcript = detail.GetProperty("transcript");

        // system + tool scaffolding dropped → only user + agent remain.
        Assert.Equal(2, transcript.GetArrayLength());
        Assert.Equal("user", transcript[0].GetProperty("role").GetString());
        Assert.Equal("hello", transcript[0].GetProperty("text").GetString());
        Assert.Equal("agent", transcript[1].GetProperty("role").GetString());      // assistant → agent
        Assert.Equal("hi there", transcript[1].GetProperty("text").GetString());   // array-of-parts extracted, not raw JSON
    }
}
