using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Metrics;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

/// <summary>
/// Coverage for <c>GET /api/apps/{slug}/usage</c> — backs the prototype's
/// <c>api.getAppUsage({appId,start,end})</c>: granularity, the conversations/tokens KPI
/// values, tokensByCapability/tokensByModel/conversationsByChannel series, and topCapabilities.
/// cdcWrites bucketing is verified purely in <see cref="AppUsageCdcWritesTests"/>; the populated
/// end-to-end CDC path needs a live source (the gated Postgres E2E lane).
/// </summary>
public class AppUsageEndpointTests(ITestOutputHelper output) : ApplianceMetricsTestBase(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task AppUsage_aggregates_conversations_tokens_and_top_capabilities()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);
        await new ConversationMetricsIndex().ExecuteAsync(store, database: perAppDb);

        var now = DateTime.UtcNow;
        await SeedConversationAsync(store, perAppDb, "chats/a", "support", now.AddDays(-1), messages: 3, tokens: 100_000);
        await SeedConversationAsync(store, perAppDb, "chats/b", "support", now.AddDays(-2), messages: 4, tokens: 200_000);
        await SeedConversationAsync(store, perAppDb, "chats/c", "sales", now.AddDays(-3), messages: 2, tokens: 50_000);
        await Indexes.WaitForIndexingAsync(store, perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var start = Uri.EscapeDataString(now.AddDays(-7).ToString("o"));
        var end = Uri.EscapeDataString(now.ToString("o"));
        var resp = await client.GetAsync($"/api/apps/my-app/usage?start={start}&end={end}");
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        var json = await resp.Content.ReadFromJsonAsync<JsonElement>();

        // 7-day range → daily granularity.
        Assert.Equal("day", json.GetProperty("granularity").GetString());

        var metrics = json.GetProperty("metrics");
        Assert.Equal(3, metrics.GetProperty("conversations").GetProperty("value").GetDouble());
        Assert.Equal(350_000, metrics.GetProperty("tokens").GetProperty("value").GetDouble());

        // topCapabilities: per-agent, sorted by totalTokens descending.
        var top = json.GetProperty("topCapabilities");
        Assert.Equal(2, top.GetArrayLength());
        Assert.Equal("support", top[0].GetProperty("name").GetString());
        Assert.Equal(300_000, top[0].GetProperty("totalTokens").GetInt64());
        Assert.Equal(2, top[0].GetProperty("invocations").GetInt64());
        Assert.Equal("sales", top[1].GetProperty("name").GetString());
        Assert.Equal(50_000, top[1].GetProperty("totalTokens").GetInt64());

        // tokensByCapability names both agents as series keys.
        var capKeys = json.GetProperty("tokensByCapability").GetProperty("keys");
        Assert.Equal(2, capKeys.GetArrayLength());

        // Nothing seeded for these in this case:
        // cdcWrites is one zero-filled point per bucket (no CDC sink in this test → all zero).
        var cdcWrites = json.GetProperty("cdcWrites");
        Assert.True(cdcWrites.GetArrayLength() > 0);
        Assert.All(cdcWrites.EnumerateArray(), p => Assert.Equal(0, p.GetProperty("writes").GetInt64()));
        Assert.Equal(0, json.GetProperty("metrics").GetProperty("cdcWrites").GetProperty("value").GetDouble());
        Assert.Equal(0, json.GetProperty("topTables").GetArrayLength());           // no business collections
        Assert.Equal(0, json.GetProperty("conversationsByChannel").GetProperty("keys").GetArrayLength()); // no iframe links
        // The conversations' agents don't match any provisioned agent → "unknown" model.
        var modelKeys = json.GetProperty("tokensByModel").GetProperty("keys");
        Assert.Equal(1, modelKeys.GetArrayLength());
        Assert.Equal("unknown", modelKeys[0].GetProperty("key").GetString());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task AppUsage_fills_model_channel_and_table_series_from_real_data()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);
        await new ConversationMetricsIndex().ExecuteAsync(store, database: perAppDb);

        // A provisioned agent (connection-string model = gpt-4o-mini) → tokensByModel.
        await SeedAgentAsync(store, perAppDb, name: "Support");
        var agents = await store.Maintenance.ForDatabase(perAppDb).SendAsync(new GetAiAgentsOperation());
        var agentId = agents.AiAgents![0].Identifier;

        var now = DateTime.UtcNow;
        await SeedConversationAsync(store, perAppDb, "chats/a", agentId, now.AddDays(-1), tokens: 100_000);
        await SeedConversationAsync(store, perAppDb, "chats/b", agentId, now.AddDays(-2), tokens: 200_000);

        // An iframe channel + two embed links → conversationsByChannel (links ≈ conversations).
        // Distinct display name so the series proves key (widgetId) vs label (displayName).
        await SeedChannelAsync(store, perAppDb, channelId: "wgt1", enabled: true, displayName: "Support Widget");
        using (var session = store.OpenAsyncSession(perAppDb))
        {
            for (var i = 0; i < 2; i++)
            {
                await session.StoreAsync(new EmbedLink
                {
                    WidgetId = "wgt1",
                    AgentId = agentId,
                    ExpiresAt = now.AddHours(1),
                    MaxInvocations = 5,
                    ConversationId = $"chats/link{i}",
                    CreatedAt = now.AddDays(-1),
                }, $"{EmbedLink.IdPrefix}{Guid.NewGuid():N}");
            }
            await session.StoreAsync(new Product { Name = "Widget" }, "products/1");  // → topTables
            await session.SaveChangesAsync();
        }

        await Indexes.WaitForIndexingAsync(store, perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var start = Uri.EscapeDataString(now.AddDays(-7).ToString("o"));
        var end = Uri.EscapeDataString(now.ToString("o"));
        var resp = await client.GetAsync($"/api/apps/my-app/usage?start={start}&end={end}");
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        var json = await resp.Content.ReadFromJsonAsync<JsonElement>();

        // tokensByModel — both conversations' agent resolves to the connection-string model.
        var modelKeys = json.GetProperty("tokensByModel").GetProperty("keys");
        Assert.Equal(1, modelKeys.GetArrayLength());
        Assert.Equal("gpt-4o-mini", modelKeys[0].GetProperty("key").GetString());

        // conversationsByChannel — the iframe channel, two links. Series key is the stable
        // widget id; label is the display name (C2 — must not key by display name).
        var channelData = json.GetProperty("conversationsByChannel");
        Assert.Equal(1, channelData.GetProperty("keys").GetArrayLength());
        Assert.Equal("wgt1", channelData.GetProperty("keys")[0].GetProperty("key").GetString());
        Assert.Equal("Support Widget", channelData.GetProperty("keys")[0].GetProperty("label").GetString());
        long channelTotal = 0;
        foreach (var point in channelData.GetProperty("points").EnumerateArray())
            channelTotal += point.GetProperty("wgt1").GetInt64();
        Assert.Equal(2, channelTotal);

        // topTables — the business collection with its doc count (lastWriteAt is
        // CDC-perf data, not rendered by the prototype; stays empty).
        var products = json.GetProperty("topTables").EnumerateArray()
            .Single(t => t.GetProperty("name").GetString() == "Products");
        Assert.Equal(1, products.GetProperty("writes").GetInt64());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task AppUsage_series_labels_use_agent_display_names()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);
        await new ConversationMetricsIndex().ExecuteAsync(store, database: perAppDb);
        await SeedAgentAsync(store, perAppDb, name: "Customer Support");
        var agents = await store.Maintenance.ForDatabase(perAppDb).SendAsync(new GetAiAgentsOperation());
        var agentId = agents.AiAgents![0].Identifier;

        var now = DateTime.UtcNow;
        await SeedConversationAsync(store, perAppDb, "chats/a", agentId, now.AddDays(-1), tokens: 100_000);
        await Indexes.WaitForIndexingAsync(store, perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var start = Uri.EscapeDataString(now.AddDays(-7).ToString("o"));
        var end = Uri.EscapeDataString(now.ToString("o"));
        var json = await client.GetFromJsonAsync<JsonElement>($"/api/apps/my-app/usage?start={start}&end={end}");

        // M2: the series key stays the stable agent id; the label is the human name.
        var capKeys = json.GetProperty("tokensByCapability").GetProperty("keys");
        Assert.Equal(1, capKeys.GetArrayLength());
        Assert.Equal(agentId, capKeys[0].GetProperty("key").GetString());
        Assert.Equal("Customer Support", capKeys[0].GetProperty("label").GetString());

        // topCapabilities renders the display name too.
        Assert.Equal("Customer Support", json.GetProperty("topCapabilities")[0].GetProperty("name").GetString());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task AppUsage_delta_excludes_previous_window_boundary()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);
        await new ConversationMetricsIndex().ExecuteAsync(store, database: perAppDb);

        // Align the window to an hour boundary so a row can sit exactly on `start`.
        var raw = DateTime.UtcNow;
        var nowHour = new DateTime(raw.Year, raw.Month, raw.Day, raw.Hour, 0, 0, DateTimeKind.Utc);
        var start = nowHour.AddDays(-1);  // window [start, nowHour]; previous window [start-1d, start)

        await SeedConversationAsync(store, perAppDb, "chats/cur", "support", start.AddHours(2), messages: 1, tokens: 10);
        await SeedConversationAsync(store, perAppDb, "chats/boundary", "support", start, messages: 1, tokens: 10);
        await SeedConversationAsync(store, perAppDb, "chats/prev", "support", start.AddHours(-2), messages: 1, tokens: 10);
        await Indexes.WaitForIndexingAsync(store, perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var startQ = Uri.EscapeDataString(start.ToString("o"));
        var endQ = Uri.EscapeDataString(nowHour.ToString("o"));
        var json = await client.GetFromJsonAsync<JsonElement>($"/api/apps/my-app/usage?start={startQ}&end={endQ}");

        var conv = json.GetProperty("metrics").GetProperty("conversations");
        Assert.Equal(2, conv.GetProperty("value").GetDouble());  // boundary + inside-current
        // M4: the boundary row belongs to the current window only, not both. delta = (2-1)/1*100.
        // (Before the fix the boundary double-counts into prev → prev=2 → delta=0.)
        Assert.Equal(100.0, conv.GetProperty("delta").GetDouble());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task AppUsage_uses_hour_granularity_for_short_ranges()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);
        await new ConversationMetricsIndex().ExecuteAsync(store, database: perAppDb);

        var now = DateTime.UtcNow;
        await SeedConversationAsync(store, perAppDb, "chats/a", "support", now.AddHours(-3), messages: 1, tokens: 1000);
        await Indexes.WaitForIndexingAsync(store, perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var start = Uri.EscapeDataString(now.AddDays(-1).ToString("o"));  // 1-day range ≤ 2 → hour
        var end = Uri.EscapeDataString(now.ToString("o"));
        var json = await client.GetFromJsonAsync<JsonElement>($"/api/apps/my-app/usage?start={start}&end={end}");

        Assert.Equal("hour", json.GetProperty("granularity").GetString());
        Assert.Equal(1000, json.GetProperty("metrics").GetProperty("tokens").GetProperty("value").GetDouble());
        var points = json.GetProperty("tokensByCapability").GetProperty("points");
        Assert.Contains("T", points[0].GetProperty("t").GetString());  // hourly bucket label (yyyy-MM-ddTHH:00)
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task AppUsage_returns_400_when_start_is_not_before_end()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var now = DateTime.UtcNow;
        var start = Uri.EscapeDataString(now.ToString("o"));            // later
        var end = Uri.EscapeDataString(now.AddDays(-7).ToString("o"));  // earlier
        var resp = await client.GetAsync($"/api/apps/my-app/usage?start={start}&end={end}");
        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);  // C3: inverted range rejected
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task AppUsage_conversationsByChannel_survives_a_widget_keyed_like_the_time_axis()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        var now = DateTime.UtcNow;
        // A channel whose WidgetId collides with the reserved "t" time-axis key, plus a normal one.
        await SeedChannelAsync(store, perAppDb, channelId: "t", enabled: true);
        await SeedChannelAsync(store, perAppDb, channelId: "alpha", enabled: true);
        using (var session = store.OpenAsyncSession(perAppDb))
        {
            await session.StoreAsync(new EmbedLink { WidgetId = "t", AgentId = "demo", ExpiresAt = now.AddHours(1), MaxInvocations = 5, ConversationId = "chats/x", CreatedAt = now.AddDays(-1) }, $"{EmbedLink.IdPrefix}{Guid.NewGuid():N}");
            await session.StoreAsync(new EmbedLink { WidgetId = "alpha", AgentId = "demo", ExpiresAt = now.AddHours(1), MaxInvocations = 5, ConversationId = "chats/y", CreatedAt = now.AddDays(-1) }, $"{EmbedLink.IdPrefix}{Guid.NewGuid():N}");
            await session.SaveChangesAsync();
        }

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var start = Uri.EscapeDataString(now.AddDays(-7).ToString("o"));
        var end = Uri.EscapeDataString(now.ToString("o"));
        var resp = await client.GetAsync($"/api/apps/my-app/usage?start={start}&end={end}");
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());  // no 500 from the "t" collision

        var byChannel = (await resp.Content.ReadFromJsonAsync<JsonElement>()).GetProperty("conversationsByChannel");

        // The colliding WidgetId "t" is dropped from the series; the normal one remains.
        var keys = byChannel.GetProperty("keys").EnumerateArray()
            .Select(k => k.GetProperty("key").GetString()).ToArray();
        Assert.Contains("alpha", keys);
        Assert.DoesNotContain("t", keys);

        // The time axis stays a string label on every bucket — never clobbered to a number.
        foreach (var p in byChannel.GetProperty("points").EnumerateArray())
            Assert.Equal(JsonValueKind.String, p.GetProperty("t").ValueKind);
    }

    private sealed class Product
    {
        public string Name { get; set; } = "";
    }
}
