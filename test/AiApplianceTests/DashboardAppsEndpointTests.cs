using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;

namespace AiApplianceTests;

/// <summary>
/// Coverage for <c>GET /api/dashboard/apps</c> — the enriched apps list (mock-api
/// <c>listApps()</c>): per-app counts, channels label, and derived status, via
/// fan-out. (CDC-dependent <c>source.type</c>/<c>tablesCount</c> are exercised by the
/// real-data path; with no CDC config they're "" / 0.)
/// </summary>
public class DashboardAppsEndpointTests(ITestOutputHelper output) : ApplianceMetricsTestBase(output)
{
    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task DashboardApps_enriches_each_app_with_counts_and_status()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);
        await SeedAgentAsync(store, perAppDb, name: "Support");
        await SeedChannelAsync(store, perAppDb, channelId: "wgt1", enabled: true);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var apps = await client.GetFromJsonAsync<JsonElement>("/api/dashboard/apps");
        var app = apps.EnumerateArray().Single(a => a.GetProperty("slug").GetString() == "my-app");

        Assert.Equal("my-app", app.GetProperty("id").GetString());               // id == slug (routing key)
        Assert.Equal(1, app.GetProperty("agentsCount").GetInt32());
        Assert.Equal(1, app.GetProperty("channelsCount").GetInt32());
        Assert.Equal("running", app.GetProperty("status").GetString());          // agents>0, channel enabled, no CDC pause
        Assert.True(app.GetProperty("documentsCount").GetInt64() >= 1);           // the channel doc, at least
        Assert.Equal("Web widget", app.GetProperty("channelsLabel").GetString()); // IFrame → Web widget
        Assert.Equal(0, app.GetProperty("tablesCount").GetInt32());               // no CDC config
        Assert.Equal("", app.GetProperty("source").GetProperty("type").GetString());
        Assert.Equal(JsonValueKind.Null, app.GetProperty("writesPerMonth").ValueKind); // no write counter
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task DashboardApps_status_is_setup_when_no_agents()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "fresh-app", database: perAppDb);

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var apps = await client.GetFromJsonAsync<JsonElement>("/api/dashboard/apps");
        var app = apps.EnumerateArray().Single(a => a.GetProperty("slug").GetString() == "fresh-app");
        Assert.Equal("setup", app.GetProperty("status").GetString());
        Assert.Equal(0, app.GetProperty("agentsCount").GetInt32());
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task DashboardApp_single_returns_enriched_app_or_404()
    {
        var store = GetDocumentStore();
        var (perAppDb, cleanup) = await CreatePerAppDatabaseAsync(store);
        using var _db = cleanup;
        await SeedAppAsync(store, slug: "my-app", database: perAppDb);
        await SeedAgentAsync(store, perAppDb, name: "Support");

        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var app = await client.GetFromJsonAsync<JsonElement>("/api/dashboard/apps/my-app");
        Assert.Equal("my-app", app.GetProperty("id").GetString());     // id == slug (N2)
        Assert.Equal("my-app", app.GetProperty("slug").GetString());
        Assert.Equal(1, app.GetProperty("agentsCount").GetInt32());

        var missing = await client.GetAsync("/api/dashboard/apps/does-not-exist");
        Assert.Equal(HttpStatusCode.NotFound, missing.StatusCode);
    }
}
