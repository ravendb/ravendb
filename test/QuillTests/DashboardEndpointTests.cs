using System.Net.Http.Json;
using System.Text.Json;
using Raven.Quill.Metrics;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

/// <summary>
/// Coverage for the global <c>GET /api/dashboard</c>: it fans out across every app database,
/// sums the conversation aggregates for the selected <c>year</c>/<c>month</c>/<c>day</c> period,
/// and reports the app count.
/// </summary>
public class DashboardEndpointTests(ITestOutputHelper output) : ApplianceMetricsTestBase(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Dashboard_sums_conversation_totals_across_apps_for_period()
    {
        var store = GetDocumentStore();

        var (db1, cleanup1) = await CreatePerAppDatabaseAsync(store);
        using var _db1 = cleanup1;
        var (db2, cleanup2) = await CreatePerAppDatabaseAsync(store);
        using var _db2 = cleanup2;

        await SeedAppAsync(store, slug: "app-one", database: db1);
        await SeedAppAsync(store, slug: "app-two", database: db2);
        await new ConversationMetricsIndex().ExecuteAsync(store, database: db1);
        await new ConversationMetricsIndex().ExecuteAsync(store, database: db2);

        var now = DateTime.UtcNow;
        var monthStart = new DateTime(now.Year, now.Month, 1, 0, 0, 0, DateTimeKind.Utc);
        // Three in the queried month across two apps, one in the previous month (excluded).
        await SeedConversationAsync(store, db1, "chats/a", "demo", monthStart.AddHours(1), tokens: 100);
        await SeedConversationAsync(store, db1, "chats/b", "demo", monthStart.AddHours(2), tokens: 100);
        await SeedConversationAsync(store, db1, "chats/prev", "demo", monthStart.AddDays(-5), tokens: 999);
        await SeedConversationAsync(store, db2, "chats/c", "demo", monthStart.AddDays(1), tokens: 50);
        await Indexes.WaitForIndexingAsync(store, db1);
        await Indexes.WaitForIndexingAsync(store, db2);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.GetAsync($"/api/dashboard?year={now.Year}&month={now.Month}");
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        var json = await resp.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal(2, json.GetProperty("apps").GetInt32());
        Assert.Equal(3, json.GetProperty("conversations").GetInt64());
        Assert.Equal(250, json.GetProperty("tokens").GetInt64());
    }
}
