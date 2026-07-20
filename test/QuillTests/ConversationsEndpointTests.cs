using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Raven.Quill.Metrics;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

/// <summary>
/// Coverage for <c>GET /api/apps/{slug}/conversations</c> (list) and <c>/conversations/{*id}</c> (detail).
/// The list reads the <see cref="ConversationPreview"/> read-model (one row per conversation); the detail
/// reads the full transcript from the AI conversation (<c>@conversations</c>).
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
        await new ConversationPreviewIndex().ExecuteAsync(store, database: perAppDb);

        var now = DateTime.UtcNow;
        // chats/recent: served through the wgt1 channel; chats/old is a direct chat. Both seed a
        // conversation, which co-writes the read-model preview the list reads.
        await SeedConversationAsync(store, perAppDb, "chats/recent", "order-support", now.AddMinutes(-10),
            turns: [("user", "hello"), ("assistant", "hi there")], channelWidgetId: "wgt1");
        await SeedConversationAsync(store, perAppDb, "chats/old", "billing", now.AddDays(-3));
        await SeedChannelAsync(store, perAppDb, channelId: "wgt1", enabled: true);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        // List — newest first, with a last-exchange preview (full transcript stays detail-only),
        // derived state/initials, channel attribution.
        var response = await client.GetFromJsonAsync<JsonElement>($"/api/apps/my-app/conversations?year={now.Year}");
        var list = response.GetProperty("conversations");
        Assert.Equal(2, list.GetArrayLength());

        var first = list[0];
        Assert.Equal("chats/recent", first.GetProperty("id").GetString());
        Assert.Equal("my-app", first.GetProperty("appId").GetString());
        Assert.Equal("order-support", first.GetProperty("agentName").GetString());
        Assert.Equal("active", first.GetProperty("state").GetString());          // 10 min ago
        var firstExchange = first.GetProperty("lastExchange");                   // last-exchange preview, newest first
        Assert.Equal(2, firstExchange.GetArrayLength());
        Assert.Equal("agent", firstExchange[0].GetProperty("role").GetString());
        Assert.Equal("hi there", firstExchange[0].GetProperty("text").GetString());
        Assert.Equal("user", firstExchange[1].GetProperty("role").GetString());
        Assert.Equal("hello", firstExchange[1].GetProperty("text").GetString());
        Assert.Equal(JsonValueKind.Null, first.GetProperty("transcript").ValueKind);
        Assert.Equal("wgt1", first.GetProperty("channelName").GetString());      // attributed via the preview's ChannelWidgetId

        Assert.Equal("closed", list[1].GetProperty("state").GetString());        // 3 days ago
        Assert.Equal("", list[1].GetProperty("channelName").GetString());        // direct chat → unattributed

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
    public async Task Conversations_list_folds_channels_into_the_query_with_no_extra_round_trips()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);
        await new ConversationPreviewIndex().ExecuteAsync(store, database: perAppDb);

        // three conversations, each served through a DISTINCT channel (real write path co-writes the preview)
        var now = DateTime.UtcNow;
        for (var i = 0; i < 3; i++)
        {
            await SeedChannelAsync(store, perAppDb, channelId: $"wgt{i}", enabled: true, displayName: $"Widget {i}");
            // strictly in the past: the current-year period clamps End to now, and the filter is `< End`
            await SeedConversationAsync(store, perAppDb, $"chats/c{i}", "demo", now.AddMinutes(-(i + 1)), channelWidgetId: $"wgt{i}");
        }
        await Indexes.WaitForIndexingAsync(store, perAppDb);

        // call the real production query on an observable session (the endpoint's own session isn't visible)
        using var session = store.OpenAsyncSession(perAppDb);
        var result = await MetricsReadService.GetConversationsAsync(
            session, "my-app", new UsagePeriod(now.Year, null, null, now), start: 0, pageSize: 50, now, CancellationToken.None);

        // every row resolves its own channel name…
        Assert.Equal(3, result.Conversations.Count);
        Assert.Equal("Widget 0", result.Conversations.Single(c => c.Id == "chats/c0").ChannelName);
        Assert.Equal("Widget 2", result.Conversations.Single(c => c.Id == "chats/c2").ChannelName);

        // …in a SINGLE round trip: the Include folded all 3 channel docs into the query, so the per-row
        // ChannelNameAsync loads are cache hits. Without the include this would be 1 + 3 requests (N+1).
        Assert.Equal(1, session.Advanced.NumberOfRequests);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Conversations_list_shows_the_stored_last_exchange_newest_first()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);
        await new ConversationPreviewIndex().ExecuteAsync(store, database: perAppDb);

        // the read-model stores the last exchange directly (no transcript scan); the row renders it newest-first
        var now = DateTime.UtcNow;
        await SeedConversationAsync(store, perAppDb, "chats/x", "agent-x", now.AddMinutes(-5),
            turns: [("user", "m13"), ("assistant", "m14")]);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var response = await client.GetFromJsonAsync<JsonElement>($"/api/apps/my-app/conversations?year={now.Year}");
        var list = response.GetProperty("conversations");
        Assert.Equal(1, list.GetArrayLength());

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
    public async Task Conversations_list_pages_by_recency_newest_first()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);
        await new ConversationPreviewIndex().ExecuteAsync(store, database: perAppDb);

        // c0 is newest (LastMessageAt = now), c4 oldest; the list pages newest-first via the index.
        var now = DateTime.UtcNow;
        for (var i = 0; i < 5; i++)
            await SeedConversationAsync(store, perAppDb, $"chats/c{i}", "demo", now.AddMinutes(-i));

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var page1 = await client.GetFromJsonAsync<JsonElement>($"/api/apps/my-app/conversations?year={now.Year}&pageSize=2");
        Assert.Equal(new[] { "chats/c0", "chats/c1" },
            page1.GetProperty("conversations").EnumerateArray().Select(x => x.GetProperty("id").GetString()).ToArray());
        Assert.Equal(5, page1.GetProperty("totalResults").GetInt64());   // full count, independent of the page

        var page2 = await client.GetFromJsonAsync<JsonElement>($"/api/apps/my-app/conversations?year={now.Year}&start=2&pageSize=2");
        Assert.Equal(new[] { "chats/c2", "chats/c3" },
            page2.GetProperty("conversations").EnumerateArray().Select(x => x.GetProperty("id").GetString()).ToArray());
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
