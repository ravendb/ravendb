using System.Text.Json;
using System.Net.Http.Json;
using Raven.AiAppliance.Metrics;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;

namespace AiApplianceTests;

/// <summary>
/// Coverage for the global <c>GET /api/dashboard</c>: it fans out across every
/// app database, sums the windowed conversation aggregates, and reports the app
/// count.
/// </summary>
public class DashboardEndpointTests(ITestOutputHelper output) : ApplianceMetricsTestBase(output)
{
    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Dashboard_sums_conversation_windows_across_apps()
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
        await SeedConversationAsync(store, db1, "chats/a", "demo", now.AddHours(-1), tokens: 100);
        await SeedConversationAsync(store, db1, "chats/b", "demo", now.AddHours(-2), tokens: 100);
        await SeedConversationAsync(store, db1, "chats/old", "demo", now.AddDays(-20), tokens: 999);
        await SeedConversationAsync(store, db2, "chats/c", "demo", now.AddHours(-3), tokens: 50);
        await Indexes.WaitForIndexingAsync(store, db1);
        await Indexes.WaitForIndexingAsync(store, db2);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.GetAsync("/api/dashboard");
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        var json = await resp.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal(2, json.GetProperty("apps").GetInt32());
        Assert.Equal(3, json.GetProperty("last24h").GetProperty("conversations").GetInt64());
        Assert.Equal(250, json.GetProperty("last24h").GetProperty("tokens").GetInt64());
        Assert.Equal(4, json.GetProperty("last30d").GetProperty("conversations").GetInt64());
    }
}
