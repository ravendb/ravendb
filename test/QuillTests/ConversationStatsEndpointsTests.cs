using System.Net.Http.Json;
using System.Text.Json;
using Raven.Quill.Metrics;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

/// <summary>
/// Coverage for the dashboard read-side <c>GET /api/apps/{slug}/conversations/stats</c>
/// endpoint: it aggregates the per-app <c>@conversations</c> collection into totals for the
/// calendar period selected by <c>year</c>/<c>month</c>/<c>day</c> (mirroring the usage
/// endpoints), via the <see cref="ConversationMetricsIndex"/> map-reduce index. No live LLM
/// is needed — conversations are seeded directly.
/// </summary>
public class ConversationStatsEndpointsTests(ITestOutputHelper output) : ApplianceMetricsTestBase(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Conversation_stats_counts_conversations_in_selected_period()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);
        await new ConversationMetricsIndex().ExecuteAsync(store, database: perAppDb);

        var now = DateTime.UtcNow;
        var monthStart = new DateTime(now.Year, now.Month, 1, 0, 0, 0, DateTimeKind.Utc);
        // Three inside the queried month, one in the previous month (excluded).
        await SeedConversationAsync(store, perAppDb, "chats/a", "demo", monthStart.AddHours(1));
        await SeedConversationAsync(store, perAppDb, "chats/b", "demo", monthStart.AddDays(1));
        await SeedConversationAsync(store, perAppDb, "chats/c", "demo", monthStart.AddDays(2));
        await SeedConversationAsync(store, perAppDb, "chats/prev", "demo", monthStart.AddDays(-3));
        await Indexes.WaitForIndexingAsync(store, perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.GetAsync($"/api/apps/my-app/conversations/stats?year={now.Year}&month={now.Month}");
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        var json = await resp.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal(3, json.GetProperty("conversations").GetInt64());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Conversation_stats_sums_messages_and_tokens_in_selected_period()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);
        await new ConversationMetricsIndex().ExecuteAsync(store, database: perAppDb);

        var now = DateTime.UtcNow;
        var monthStart = new DateTime(now.Year, now.Month, 1, 0, 0, 0, DateTimeKind.Utc);
        await SeedConversationAsync(store, perAppDb, "chats/a", "demo", monthStart.AddHours(1), messages: 3, tokens: 100);
        await SeedConversationAsync(store, perAppDb, "chats/b", "demo", monthStart.AddHours(2), messages: 5, tokens: 250);
        // Previous month — excluded from the queried period.
        await SeedConversationAsync(store, perAppDb, "chats/prev", "demo", monthStart.AddDays(-3), messages: 9, tokens: 999);
        await Indexes.WaitForIndexingAsync(store, perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.GetAsync($"/api/apps/my-app/conversations/stats?year={now.Year}&month={now.Month}");
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        var json = await resp.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal(2, json.GetProperty("conversations").GetInt64());
        Assert.Equal(8, json.GetProperty("messages").GetInt64());
        Assert.Equal(350, json.GetProperty("tokens").GetInt64());
    }
}
