using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

/// <summary>
/// Coverage for the settings surfaces — <c>GET /api/settings/license</c> and
/// <c>GET /api/settings/usage</c>. Both are RavenDB-backed (see
/// <c>LicenseStatsProvider</c>): license proxies the server's <c>/license/status</c> +
/// <c>/license-server/connectivity</c> and appends the static plan catalog; usage
/// proxies <c>/license/quill/usage</c>. Assertions target the response shape and
/// environment-stable fields, not license-specific values (which vary with whatever
/// license the test server runs under).
/// </summary>
public class SettingsEndpointsTests(ITestOutputHelper output) : ApplianceMetricsTestBase(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task License_surfaces_server_license_connectivity_and_plans()
    {
        var store = GetDocumentStore();
        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var license = await client.GetFromJsonAsync<JsonElement>("/api/settings/license");

        // response: the server's /license/status, projected onto ServerLicenseResponse.
        var response = license.GetProperty("response");
        Assert.False(string.IsNullOrEmpty(response.GetProperty("status").GetString()));  // e.g. "Commercial" / "AGPL - Open Source"
        Assert.False(string.IsNullOrEmpty(response.GetProperty("type").GetString()));     // e.g. "EnterpriseAi" / "None"
        Assert.True(response.GetProperty("expired").ValueKind is JsonValueKind.True or JsonValueKind.False);

        // connectivity: the server's /license-server/connectivity probe.
        Assert.False(string.IsNullOrEmpty(license.GetProperty("connectivity").GetProperty("statusCode").GetString()));

        // plans: the static catalog LicenseStatsProvider always appends.
        var plans = license.GetProperty("plans");
        Assert.True(plans.GetArrayLength() >= 1);
        Assert.Equal("enterprise", plans[0].GetProperty("slug").GetString());
        Assert.True(plans[0].GetProperty("features").GetArrayLength() >= 1);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Usage_returns_quill_usage_payload()
    {
        var store = GetDocumentStore();
        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        var resp = await client.GetAsync("/api/settings/usage?year=2026&month=5");
        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);

        var usage = await resp.Content.ReadFromJsonAsync<JsonElement>();
        // QuillUsageResponse { perApplication, byPeriod } — both may be null when the
        // server reports no usage, but the shape must always be present.
        Assert.True(usage.TryGetProperty("perApplication", out _));
        Assert.True(usage.TryGetProperty("byPeriod", out _));
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Usage_forwards_month_without_client_side_validation()
    {
        var store = GetDocumentStore();
        using var factory = NewApplianceFactory(store);
        var client = factory.CreateClient();

        // The endpoint forwards year/month straight to RavenDB's /license/quill/usage;
        // it does not reject out-of-range months itself (contrast the former mock, which
        // 400'd on month=13). Characterizes current behavior — see note if validation
        // should move back into the appliance.
        var resp = await client.GetAsync("/api/settings/usage?year=2026&month=13");
        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);
    }
}
