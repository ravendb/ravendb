using System.Net.Http.Json;
using System.Text.Json;
using Raven.AiAppliance.Metrics;
using Tests.Infrastructure;
using Xunit;

namespace AiApplianceTests;

/// <summary>
/// Coverage for <c>GET /api/usage/by-app</c> — backs the prototype's
/// <c>api.getTokensByApp()</c>. Contract (mock-api.ts): <c>{ apps: [{ slug, tokens }],
/// refreshedMinutesAgo }</c> — one row per app with all-time token usage summed from
/// its <c>@conversations</c>, sorted by tokens descending. Aggregated via fan-out over
/// the per-app <see cref="ConversationMetricsIndex"/>; no live LLM.
/// </summary>
public class TokensByAppEndpointTests(ITestOutputHelper output) : ApplianceMetricsTestBase(output)
{
    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task TokensByApp_sums_tokens_per_app_sorted_descending()
    {
        var store = GetDocumentStore();
        var (appDb1, cleanup1) = await CreatePerAppDatabaseAsync(store);
        using var _db1 = cleanup1;
        var (appDb2, cleanup2) = await CreatePerAppDatabaseAsync(store);
        using var _db2 = cleanup2;
        await SeedAppAsync(store, slug: "app-one", database: appDb1);
        await SeedAppAsync(store, slug: "app-two", database: appDb2);
        await new ConversationMetricsIndex().ExecuteAsync(store, database: appDb1);
        await new ConversationMetricsIndex().ExecuteAsync(store, database: appDb2);

        var now = DateTime.UtcNow;
        // app-one: 100 + 50 across two hours (and an old one) — all-time, no window.
        await SeedConversationAsync(store, appDb1, "chats/a", "demo", now.AddHours(-1), tokens: 100);
        await SeedConversationAsync(store, appDb1, "chats/b", "demo", now.AddDays(-10), tokens: 50);
        await SeedConversationAsync(store, appDb2, "chats/c", "demo", now.AddHours(-2), tokens: 400);
        await Indexes.WaitForIndexingAsync(store, appDb1);
        await Indexes.WaitForIndexingAsync(store, appDb2);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.GetAsync("/api/usage/by-app");
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        var json = await resp.Content.ReadFromJsonAsync<JsonElement>();
        Assert.True(json.TryGetProperty("refreshedMinutesAgo", out _), "missing 'refreshedMinutesAgo'");

        var apps = json.GetProperty("apps");
        Assert.Equal(2, apps.GetArrayLength());

        // Sorted by tokens descending: app-two (400) before app-one (150 = 100 + 50, all-time).
        Assert.Equal("app-two", apps[0].GetProperty("slug").GetString());
        Assert.Equal(400, apps[0].GetProperty("tokens").GetInt64());
        Assert.Equal("app-one", apps[1].GetProperty("slug").GetString());
        Assert.Equal(150, apps[1].GetProperty("tokens").GetInt64());
    }
}
