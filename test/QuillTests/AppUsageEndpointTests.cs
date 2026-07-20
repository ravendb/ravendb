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
/// <c>api.getAppUsage({appId,from,window})</c>: the conversations/tokens KPI
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

        // Seed inside the current month so the ByDay window (this month) aggregates them.
        var now = DateTime.UtcNow;
        await SeedConversationAsync(store, perAppDb, "chats/a", "support", now, messages: 3, tokens: 100_000);
        await SeedConversationAsync(store, perAppDb, "chats/b", "support", now, messages: 4, tokens: 200_000);
        await SeedConversationAsync(store, perAppDb, "chats/c", "sales", now, messages: 2, tokens: 50_000);
        await Indexes.WaitForIndexingAsync(store, perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var json = await client.GetFromJsonAsync<JsonElement>($"/api/apps/my-app/usage?{Range(now.Year, now.Month)}");

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
        await SeedConversationAsync(store, perAppDb, "chats/a", agentId, now, tokens: 100_000);
        await SeedConversationAsync(store, perAppDb, "chats/b", agentId, now, tokens: 200_000);

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
                    CreatedAt = now,
                }, $"{EmbedLink.IdPrefix}{Guid.NewGuid():N}");
            }
            await session.StoreAsync(new Product { Name = "Widget" }, "products/1");  // → topTables
            await session.SaveChangesAsync();
        }

        await Indexes.WaitForIndexingAsync(store, perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var json = await client.GetFromJsonAsync<JsonElement>($"/api/apps/my-app/usage?{Range(now.Year, now.Month)}");

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
        await SeedConversationAsync(store, perAppDb, "chats/a", agentId, now, tokens: 100_000);
        await Indexes.WaitForIndexingAsync(store, perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var json = await client.GetFromJsonAsync<JsonElement>($"/api/apps/my-app/usage?{Range(now.Year, now.Month)}");

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

        // ByHour window: current period = today [start, tomorrow); previous = yesterday [start-1d, start).
        var now = DateTime.UtcNow;
        var start = new DateTime(now.Year, now.Month, now.Day, 0, 0, 0, DateTimeKind.Utc);

        await SeedConversationAsync(store, perAppDb, "chats/cur", "support", start.AddHours(Math.Min(now.Hour, 2)), messages: 1, tokens: 10);
        await SeedConversationAsync(store, perAppDb, "chats/boundary", "support", start, messages: 1, tokens: 10);
        await SeedConversationAsync(store, perAppDb, "chats/prev", "support", start.AddHours(-2), messages: 1, tokens: 10);
        await Indexes.WaitForIndexingAsync(store, perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var json = await client.GetFromJsonAsync<JsonElement>($"/api/apps/my-app/usage?{Range(now.Year, now.Month, now.Day)}");

        var conv = json.GetProperty("metrics").GetProperty("conversations");
        Assert.Equal(2, conv.GetProperty("value").GetDouble());  // boundary + inside-current
        // M4: the boundary row (bucket == start) belongs to the current window only, not the
        // previous one, so prev = 1 and delta = (2-1)/1*100.
        Assert.Equal(100.0, conv.GetProperty("delta").GetDouble());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task AppUsage_uses_hour_buckets_for_the_ByHour_window()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);
        await new ConversationMetricsIndex().ExecuteAsync(store, database: perAppDb);

        var now = DateTime.UtcNow;
        await SeedConversationAsync(store, perAppDb, "chats/a", "support", now, messages: 1, tokens: 1000);
        await Indexes.WaitForIndexingAsync(store, perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var json = await client.GetFromJsonAsync<JsonElement>($"/api/apps/my-app/usage?{Range(now.Year, now.Month, now.Day)}");

        Assert.Equal(1000, json.GetProperty("metrics").GetProperty("tokens").GetProperty("value").GetDouble());
        var points = json.GetProperty("tokensByCapability").GetProperty("points");
        Assert.Contains("T", points[0].GetProperty("t").GetString());  // hourly bucket label (yyyy-MM-ddTHH:00)
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
            await session.StoreAsync(new EmbedLink { WidgetId = "t", AgentId = "demo", ExpiresAt = now.AddHours(1), MaxInvocations = 5, ConversationId = "chats/x", CreatedAt = now }, $"{EmbedLink.IdPrefix}{Guid.NewGuid():N}");
            await session.StoreAsync(new EmbedLink { WidgetId = "alpha", AgentId = "demo", ExpiresAt = now.AddHours(1), MaxInvocations = 5, ConversationId = "chats/y", CreatedAt = now }, $"{EmbedLink.IdPrefix}{Guid.NewGuid():N}");
            await session.SaveChangesAsync();
        }

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.GetAsync($"/api/apps/my-app/usage?{Range(now.Year, now.Month)}");
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

    // The endpoint takes ?year={y}[&month={m}][&day={d}]; the granularity follows which fields
    // are set (year → months, +month → days, +month+day → hours).
    private static string Range(int year, int? month = null, int? day = null)
    {
        var q = $"year={year}";
        if (month is not null) q += $"&month={month}";
        if (day is not null) q += $"&day={day}";
        return q;
    }

    private sealed class Product
    {
        public string Name { get; set; } = "";
    }
}
