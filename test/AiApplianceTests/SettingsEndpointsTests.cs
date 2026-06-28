using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Tests.Infrastructure;
using Xunit;

namespace AiApplianceTests;

/// <summary>
/// Coverage for the settings surfaces — <c>GET /api/settings/license</c> (mock-api
/// <c>getLicense</c>) and <c>GET /api/settings/usage</c> (<c>getMonthlyWrites</c>).
/// Both are mock-backed (no real license API yet), so no RavenDB seeding is needed —
/// just the hosted appliance.
/// </summary>
public class SettingsEndpointsTests(ITestOutputHelper output) : ApplianceMetricsTestBase(output)
{
    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task License_returns_trial_by_default_and_expired_on_demo_state()
    {
        var store = GetDocumentStore();
        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var healthy = await client.GetFromJsonAsync<JsonElement>("/api/settings/license");
        Assert.Equal("healthy", healthy.GetProperty("state").GetString());
        Assert.Equal("Trial", healthy.GetProperty("tier").GetString());
        Assert.True(healthy.GetProperty("apiHealthy").GetBoolean());
        Assert.True(healthy.GetProperty("plans").GetArrayLength() >= 1);
        Assert.True(healthy.GetProperty("includes").GetArrayLength() >= 1);
        Assert.Equal(JsonValueKind.Null, healthy.GetProperty("graceHoursLeft").ValueKind);

        var expired = await client.GetFromJsonAsync<JsonElement>("/api/settings/license?demoState=expired");
        Assert.Equal("expired", expired.GetProperty("state").GetString());
        Assert.Equal("Expired", expired.GetProperty("tier").GetString());
        Assert.Equal(14, expired.GetProperty("graceHoursLeft").GetInt32());
        Assert.True(expired.GetProperty("stops").GetArrayLength() >= 1);
        Assert.True(expired.GetProperty("keeps").GetArrayLength() >= 1);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task MonthlyWrites_returns_quota_and_daily_breakdown()
    {
        var store = GetDocumentStore();
        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var usage = await client.GetFromJsonAsync<JsonElement>("/api/settings/usage?year=2026&month=5");
        Assert.Equal(2_000_000, usage.GetProperty("monthlyQuota").GetInt64());
        Assert.Equal(31, usage.GetProperty("days").GetArrayLength());  // May has 31 days
        Assert.True(usage.GetProperty("monthlyUsed").GetInt64() > 0);
        Assert.Equal("May 2026", usage.GetProperty("monthLabel").GetString());
        Assert.Equal("2026-05-01", usage.GetProperty("days")[0].GetProperty("date").GetString());
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task MonthlyWrites_rejects_invalid_month()
    {
        var store = GetDocumentStore();
        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.GetAsync("/api/settings/usage?year=2026&month=13");
        Assert.Equal(HttpStatusCode.BadRequest, resp.StatusCode);
    }
}
