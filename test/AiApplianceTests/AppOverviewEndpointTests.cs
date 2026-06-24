using System.Text.Json;
using System.Net.Http.Json;
using Tests.Infrastructure;
using Xunit;

namespace AiApplianceTests;

/// <summary>
/// Coverage for <c>GET /api/apps/{slug}/overview</c>: an index-free snapshot of
/// the app's document volume, configured-agent count, and channel counts for
/// the App Overview view.
/// </summary>
public class AppOverviewEndpointTests(ITestOutputHelper output) : ApplianceMetricsTestBase(output)
{
    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task App_overview_reports_documents_agents_and_channels()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        await SeedAgentAsync(store, perAppDb, name: "Support");
        await SeedAgentAsync(store, perAppDb, name: "Sales");
        await SeedChannelAsync(store, perAppDb, "alpha", enabled: true);
        await SeedChannelAsync(store, perAppDb, "beta", enabled: true);
        await SeedChannelAsync(store, perAppDb, "gamma", enabled: false);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.GetAsync("/api/apps/my-app/overview");
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        var json = await resp.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal("my-app", json.GetProperty("slug").GetString());
        Assert.Equal(2, json.GetProperty("configuredAgents").GetInt32());
        Assert.Equal(3, json.GetProperty("channels").GetInt32());
        Assert.Equal(2, json.GetProperty("activeChannels").GetInt32());
        Assert.True(json.GetProperty("documents").GetInt64() >= 3);
    }
}
