using System.Net.Http.Json;
using System.Text.Json;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

/// <summary>
/// Coverage for <c>GET /api/apps/{slug}/channels/stats</c>: total and active
/// (enabled) channel counts for the Channels view. Channels are read by id
/// prefix (immediately consistent), so no index/staleness wait is needed.
/// </summary>
public class ChannelStatsEndpointsTests(ITestOutputHelper output) : ApplianceMetricsTestBase(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Channel_stats_counts_total_and_active()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        await SeedChannelAsync(store, perAppDb, "alpha", enabled: true);
        await SeedChannelAsync(store, perAppDb, "beta", enabled: true);
        await SeedChannelAsync(store, perAppDb, "gamma", enabled: false);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.GetAsync("/api/apps/my-app/channels/stats");
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        var json = await resp.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal(3, json.GetProperty("total").GetInt32());
        Assert.Equal(2, json.GetProperty("active").GetInt32());
    }
}
