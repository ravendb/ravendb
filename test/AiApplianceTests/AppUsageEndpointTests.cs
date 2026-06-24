using System.Net.Http.Json;
using System.Text.Json;
using Raven.AiAppliance.Metrics;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;

namespace AiApplianceTests;

/// <summary>
/// Coverage for <c>GET /api/apps/{slug}/usage</c> — backs the prototype's
/// <c>api.getAppUsage({appId,start,end})</c>. Phase-1 subset: granularity, the
/// conversations/tokens/cost KPI values, tokensByCapability keys, and topCapabilities
/// from the per-app <see cref="ConversationMetricsIndex"/>. CDC/model/channel fields
/// ship as empty skeletons (no source yet).
/// </summary>
public class AppUsageEndpointTests(ITestOutputHelper output) : ApplianceMetricsTestBase(output)
{
    private const double CostPerToken = 0.000015;

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task AppUsage_aggregates_conversations_tokens_cost_and_top_capabilities()
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
        Assert.Equal(350_000 * CostPerToken, metrics.GetProperty("cost").GetProperty("value").GetDouble(), 3);

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

        // Skeleton fields (no source yet) — present and empty.
        Assert.Equal(0, json.GetProperty("cdcWrites").GetArrayLength());
        Assert.Equal(0, json.GetProperty("topTables").GetArrayLength());
        Assert.Equal(0, json.GetProperty("tokensByModel").GetProperty("keys").GetArrayLength());
        Assert.Equal(0, json.GetProperty("conversationsByChannel").GetProperty("keys").GetArrayLength());
    }
}
