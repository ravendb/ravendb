using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using AiApplianceTests.E2E.Fixtures;
using FastTests;
using Raven.AiAppliance.Auth;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;

namespace AiApplianceTests;

/// <summary>
/// Auth-gate coverage for the appliance admin surface. Every <c>/api/*</c> admin endpoint requires
/// either the API-key header (the <c>api.*</c> credential) or a login-issued session cookie (the
/// <c>dashboard.*</c> credential), both validated against <c>QUILL_API_KEY</c>
/// (<see cref="ApplianceWebApplicationFactory.TestApiKey"/> here).
/// </summary>
public class AuthEndpointsTests(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Admin_endpoint_without_credential_is_401()
    {
        var store = GetDocumentStore();
        using var factory = NewFactory(store);
        var client = factory.CreateClient();
        client.DefaultRequestHeaders.Remove(ApiKeyAuthenticationHandler.HeaderName);

        var resp = await client.GetAsync("/api/apps");
        Assert.Equal(HttpStatusCode.Unauthorized, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Admin_endpoint_with_api_key_header_is_authorized()
    {
        var store = GetDocumentStore();
        using var factory = NewFactory(store);
        var client = factory.CreateClient(); // carries the X-Api-Key header by default

        var resp = await client.GetAsync("/api/apps");
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Login_with_wrong_key_is_401()
    {
        var store = GetDocumentStore();
        using var factory = NewFactory(store);
        var client = factory.CreateClient();
        client.DefaultRequestHeaders.Remove(ApiKeyAuthenticationHandler.HeaderName);

        var resp = await client.PostAsJsonAsync("/api/auth/login", new { apiKey = "wrong-key" });
        Assert.Equal(HttpStatusCode.Unauthorized, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.AiAppliance)]
    public async Task Login_with_correct_key_sets_session_cookie_that_authorizes()
    {
        var store = GetDocumentStore();
        using var factory = NewFactory(store);
        var client = factory.CreateClient();
        // Drop the header so the follow-up call's success is attributable to the session cookie alone.
        client.DefaultRequestHeaders.Remove(ApiKeyAuthenticationHandler.HeaderName);

        var login = await client.PostAsJsonAsync("/api/auth/login",
            new { apiKey = ApplianceWebApplicationFactory.TestApiKey });
        Assert.True(login.IsSuccessStatusCode, await login.Content.ReadAsStringAsync());

        // The test client persists the Set-Cookie; the gated call now succeeds on the session alone.
        var apps = await client.GetAsync("/api/apps");
        Assert.True(apps.IsSuccessStatusCode, await apps.Content.ReadAsStringAsync());

        var status = await client.GetFromJsonAsync<JsonElement>("/api/auth/status");
        Assert.True(status.GetProperty("authenticated").GetBoolean());
    }

    private ApplianceWebApplicationFactory NewFactory(IDocumentStore store) =>
        new(licenseApiUrl: "http://unused-in-unit-tests",
            setupPackagePath: NewDataPath(forceCreateDir: true),
            applianceStore: store,
            configureOptions: opts => opts.ConfigDatabase = store.Database);
}
