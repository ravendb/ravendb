using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using QuillTests.E2E.Fixtures;
using Raven.Quill.Auth;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class AuthEndpointsTests(ITestOutputHelper output) : QuillTestBase(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Admin_endpoint_without_credential_is_401()
    {
        // throwaway client so the shared Host.Client isn't corrupted
        using var client = Host.Factory.CreateClient();
        client.DefaultRequestHeaders.Remove(ApiKeyAuthenticationHandler.HeaderName);

        var resp = await client.GetAsync(QuillRoutes.Apps);
        Assert.Equal(HttpStatusCode.Unauthorized, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Admin_endpoint_with_api_key_header_is_authorized()
    {
        var resp = await Host.Client.GetAsync(QuillRoutes.Apps);
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Login_with_wrong_key_is_401()
    {
        await using var host = await NewHostAsync();
        host.Client.DefaultRequestHeaders.Remove(ApiKeyAuthenticationHandler.HeaderName);

        var resp = await host.Client.PostAsJsonAsync(QuillRoutes.AuthLogin, new { apiKey = "wrong-key" });
        Assert.Equal(HttpStatusCode.Unauthorized, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Login_with_correct_key_sets_session_cookie_that_authorizes()
    {
        await using var host = await NewHostAsync();
        // header dropped so the follow-up's success is attributable to the session cookie alone
        host.Client.DefaultRequestHeaders.Remove(ApiKeyAuthenticationHandler.HeaderName);

        var login = await host.Client.PostAsJsonAsync(QuillRoutes.AuthLogin,
            new { apiKey = ApplianceWebApplicationFactory.TestApiKey });
        Assert.True(login.IsSuccessStatusCode, await login.Content.ReadAsStringAsync());

        var apps = await host.Client.GetAsync(QuillRoutes.Apps);
        Assert.True(apps.IsSuccessStatusCode, await apps.Content.ReadAsStringAsync());

        var status = await host.GetAuthStatusAsync();
        Assert.True(status.Authenticated);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task With_no_operator_key_configured_even_a_plausible_key_is_401()
    {
        await using var host = await NewHostAsync(configure: opts => opts.ApiKey = null, seedChatConnectionString: false);
        var client = host.Client; // carries the default valid-looking X-Api-Key header, yet still 401

        var resp = await client.GetAsync(QuillRoutes.Apps);
        Assert.Equal(HttpStatusCode.Unauthorized, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Admin_endpoint_with_bearer_token_is_authorized()
    {
        using var client = Host.Factory.CreateClient();
        client.DefaultRequestHeaders.Remove(ApiKeyAuthenticationHandler.HeaderName);
        client.DefaultRequestHeaders.Authorization =
            new AuthenticationHeaderValue("Bearer", ApplianceWebApplicationFactory.TestApiKey);

        var resp = await client.GetAsync(QuillRoutes.Apps);
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Login_is_rate_limited_after_repeated_attempts()
    {
        await using var host = await NewHostAsync();
        host.Client.DefaultRequestHeaders.Remove(ApiKeyAuthenticationHandler.HeaderName);

        var statuses = new List<HttpStatusCode>();
        for (var i = 0; i < 12; i++)
        {
            var resp = await host.Client.PostAsJsonAsync(QuillRoutes.AuthLogin, new { apiKey = "wrong-key" });
            statuses.Add(resp.StatusCode);
        }

        Assert.Contains(HttpStatusCode.TooManyRequests, statuses);
    }
}
