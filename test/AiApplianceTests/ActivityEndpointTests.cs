using System.Net.Http.Json;
using System.Text.Json;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;

namespace AiApplianceTests;

/// <summary>
/// <c>GET /api/apps/{slug}/activity</c> (mock-api <c>listActivity</c>) is a deferred
/// stub: there's no event-log source yet, so it returns an empty feed.
/// </summary>
public class ActivityEndpointTests(ITestOutputHelper output) : ApplianceMetricsTestBase(output)
{
    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Activity_returns_empty_feed()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.GetAsync("/api/apps/my-app/activity");
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());

        var json = await resp.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal(JsonValueKind.Array, json.ValueKind);
        Assert.Equal(0, json.GetArrayLength());
    }
}
