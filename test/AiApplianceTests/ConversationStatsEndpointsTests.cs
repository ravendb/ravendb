using System.Text.Json;
using System.Net.Http.Json;
using Raven.AiAppliance.Metrics;
using Tests.Infrastructure;
using Xunit;

namespace AiApplianceTests;

/// <summary>
/// Coverage for the dashboard read-side <c>GET /api/apps/{slug}/conversations/stats</c>
/// endpoint: it aggregates the per-app <c>@conversations</c> collection into
/// rolling windows (last 24h / 7d / 30d) via the <see cref="ConversationMetricsIndex"/>
/// map-reduce index. No live LLM is needed — conversations are seeded directly.
/// </summary>
public class ConversationStatsEndpointsTests(ITestOutputHelper output) : ApplianceMetricsTestBase(output)
{
    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Conversation_stats_counts_conversations_per_window()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);
        await new ConversationMetricsIndex().ExecuteAsync(store, database: perAppDb);

        var now = DateTime.UtcNow;
        await SeedConversationAsync(store, perAppDb, "chats/a", "demo", now.AddHours(-1));
        await SeedConversationAsync(store, perAppDb, "chats/b", "demo", now.AddDays(-3));
        await SeedConversationAsync(store, perAppDb, "chats/c", "demo", now.AddDays(-20));
        await Indexes.WaitForIndexingAsync(store, perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.GetAsync("/api/apps/my-app/conversations/stats");
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        var json = await resp.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal(1, json.GetProperty("last24h").GetProperty("conversations").GetInt64());
        Assert.Equal(2, json.GetProperty("last7d").GetProperty("conversations").GetInt64());
        Assert.Equal(3, json.GetProperty("last30d").GetProperty("conversations").GetInt64());
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Conversation_stats_sums_messages_and_tokens_per_window()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);
        await new ConversationMetricsIndex().ExecuteAsync(store, database: perAppDb);

        var now = DateTime.UtcNow;
        await SeedConversationAsync(store, perAppDb, "chats/a", "demo", now.AddHours(-1), messages: 3, tokens: 100);
        await SeedConversationAsync(store, perAppDb, "chats/b", "demo", now.AddHours(-2), messages: 5, tokens: 250);
        await SeedConversationAsync(store, perAppDb, "chats/old", "demo", now.AddDays(-20), messages: 9, tokens: 999);
        await Indexes.WaitForIndexingAsync(store, perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.GetAsync("/api/apps/my-app/conversations/stats");
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        var json = await resp.Content.ReadFromJsonAsync<JsonElement>();
        var last24h = json.GetProperty("last24h");
        Assert.Equal(2, last24h.GetProperty("conversations").GetInt64());
        Assert.Equal(8, last24h.GetProperty("messages").GetInt64());
        Assert.Equal(350, last24h.GetProperty("tokens").GetInt64());
    }
}
