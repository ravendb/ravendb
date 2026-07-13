using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text.Json;
using FastTests;
using QuillTests.E2E.Fixtures;
using Raven.Client.Documents;
using Raven.Quill.Auth;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

/// <summary>
/// Auth-gate coverage for the appliance admin surface. Every <c>/api/*</c> admin endpoint requires
/// either the API-key header (the <c>api.*</c> credential) or a login-issued session cookie (the
/// <c>dashboard.*</c> credential), both validated against <c>QUILL_API_KEY</c>
/// (<see cref="ApplianceWebApplicationFactory.TestApiKey"/> here).
/// </summary>
public class AuthEndpointsTests(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Admin_endpoint_without_credential_is_401()
    {
        var store = GetDocumentStore();
        using var factory = NewFactory(store);
        var client = factory.CreateClient();
        client.DefaultRequestHeaders.Remove(ApiKeyAuthenticationHandler.HeaderName);

        var resp = await client.GetAsync("/api/apps");
        Assert.Equal(HttpStatusCode.Unauthorized, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Admin_endpoint_with_api_key_header_is_authorized()
    {
        var store = GetDocumentStore();
        using var factory = NewFactory(store);
        var client = factory.CreateClient(); // carries the X-Api-Key header by default

        var resp = await client.GetAsync("/api/apps");
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Login_with_wrong_key_is_401()
    {
        var store = GetDocumentStore();
        using var factory = NewFactory(store);
        var client = factory.CreateClient();
        client.DefaultRequestHeaders.Remove(ApiKeyAuthenticationHandler.HeaderName);

        var resp = await client.PostAsJsonAsync("/api/auth/login", new { apiKey = "wrong-key" });
        Assert.Equal(HttpStatusCode.Unauthorized, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
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

    [RavenFact(RavenTestCategory.Quill)]
    public async Task With_no_operator_key_configured_even_a_plausible_key_is_401()
    {
        var store = GetDocumentStore();
        using var factory = new ApplianceWebApplicationFactory(
            licenseApiUrl: "http://unused-in-unit-tests",
            setupPackagePath: NewDataPath(forceCreateDir: true),
            applianceStore: store,
            configureOptions: opts =>
            {
                opts.ConfigDatabase = store.Database;
                opts.ApiKey = null; // QUILL_API_KEY unset -> ApiKeyStore fails closed (empty key set).
            });
        var client = factory.CreateClient(); // carries the default X-Api-Key header

        var resp = await client.GetAsync("/api/apps");
        Assert.Equal(HttpStatusCode.Unauthorized, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Admin_endpoint_with_bearer_token_is_authorized()
    {
        var store = GetDocumentStore();
        using var factory = NewFactory(store);
        var client = factory.CreateClient();
        // Swap the X-Api-Key header for Authorization: Bearer <key> to exercise the Bearer extraction path.
        client.DefaultRequestHeaders.Remove(ApiKeyAuthenticationHandler.HeaderName);
        client.DefaultRequestHeaders.Authorization =
            new AuthenticationHeaderValue("Bearer", ApplianceWebApplicationFactory.TestApiKey);

        var resp = await client.GetAsync("/api/apps");
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Login_is_rate_limited_after_repeated_attempts()
    {
        var store = GetDocumentStore();
        using var factory = NewFactory(store);
        var client = factory.CreateClient();
        client.DefaultRequestHeaders.Remove(ApiKeyAuthenticationHandler.HeaderName);

        // auth-login is a fixed-window 10/min policy; an attempt past the window's permit limit is 429.
        var statuses = new List<HttpStatusCode>();
        for (var i = 0; i < 12; i++)
        {
            var resp = await client.PostAsJsonAsync("/api/auth/login", new { apiKey = "wrong-key" });
            statuses.Add(resp.StatusCode);
        }

        Assert.Contains(HttpStatusCode.TooManyRequests, statuses);
    }

    private ApplianceWebApplicationFactory NewFactory(IDocumentStore store) =>
        new(licenseApiUrl: "http://unused-in-unit-tests",
            setupPackagePath: NewDataPath(forceCreateDir: true),
            applianceStore: store,
            configureOptions: opts => opts.ConfigDatabase = store.Database);
}
