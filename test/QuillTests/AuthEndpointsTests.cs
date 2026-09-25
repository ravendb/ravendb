using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Security.Cryptography;
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
    public async Task Primary_prefix_on_the_presented_key_authenticates()
    {
        await using var host = await NewHostAsync();
        host.Client.DefaultRequestHeaders.Remove(ApiKeyAuthenticationHandler.HeaderName);

        var resp = await host.Client.PostAsJsonAsync(QuillRoutes.AuthLogin,
            new { apiKey = "primary/" + ApplianceWebApplicationFactory.TestApiKey });
        Assert.True(resp.IsSuccessStatusCode, await resp.Content.ReadAsStringAsync());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Unknown_key_id_with_the_primary_secret_is_401()
    {
        await using var host = await NewHostAsync();
        host.Client.DefaultRequestHeaders.Remove(ApiKeyAuthenticationHandler.HeaderName);

        var resp = await host.Client.PostAsJsonAsync(QuillRoutes.AuthLogin,
            new { apiKey = "other/" + ApplianceWebApplicationFactory.TestApiKey });
        Assert.Equal(HttpStatusCode.Unauthorized, resp.StatusCode);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Malformed_key_id_is_401()
    {
        await using var host = await NewHostAsync();
        host.Client.DefaultRequestHeaders.Remove(ApiKeyAuthenticationHandler.HeaderName);

        foreach (var keyId in new[] { "..", "prim ary", "primary|x", new string('a', 65) })
        {
            var resp = await host.Client.PostAsJsonAsync(QuillRoutes.AuthLogin,
                new { apiKey = keyId + "/" + ApplianceWebApplicationFactory.TestApiKey });
            Assert.Equal(HttpStatusCode.Unauthorized, resp.StatusCode);
        }
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Revoking_primary_in_the_config_database_takes_effect_without_restart()
    {
        await using var host = await NewHostAsync(configure: opts => opts.ApiKeyCacheDuration = TimeSpan.Zero);
        host.Client.DefaultRequestHeaders.Remove(ApiKeyAuthenticationHandler.HeaderName);

        var before = await host.Client.PostAsJsonAsync(QuillRoutes.AuthLogin,
            new { apiKey = ApplianceWebApplicationFactory.TestApiKey });
        Assert.True(before.IsSuccessStatusCode, await before.Content.ReadAsStringAsync());

        await SetPrimaryRevokedAsync(host, revoked: true);
        var revoked = await host.Client.PostAsJsonAsync(QuillRoutes.AuthLogin,
            new { apiKey = ApplianceWebApplicationFactory.TestApiKey });
        Assert.Equal(HttpStatusCode.Unauthorized, revoked.StatusCode);

        await SetPrimaryRevokedAsync(host, revoked: false);
        var restored = await host.Client.PostAsJsonAsync(QuillRoutes.AuthLogin,
            new { apiKey = ApplianceWebApplicationFactory.TestApiKey });
        Assert.True(restored.IsSuccessStatusCode, await restored.Content.ReadAsStringAsync());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Additional_key_document_authenticates_by_its_id_without_restart()
    {
        await using var host = await NewHostAsync(configure: opts => opts.ApiKeyCacheDuration = TimeSpan.Zero);
        host.Client.DefaultRequestHeaders.Remove(ApiKeyAuthenticationHandler.HeaderName);

        const string secondSecret = "second-secret-with-plenty-of-entropy";
        var salt = RandomNumberGenerator.GetBytes(16);
        using (var session = host.Config.OpenAsyncSession())
        {
            await session.StoreAsync(new ApiKey
            {
                Label = "second",
                Salt = Convert.ToBase64String(salt),
                Hash = Convert.ToBase64String(ApiKeyStore.HashSecret(salt, secondSecret)),
                CreatedAt = DateTime.UtcNow,
            }, ApiKey.IdPrefix + "second");
            await session.SaveChangesAsync();
        }

        var prefixed = await host.Client.PostAsJsonAsync(QuillRoutes.AuthLogin, new { apiKey = "second/" + secondSecret });
        Assert.True(prefixed.IsSuccessStatusCode, await prefixed.Content.ReadAsStringAsync());

        var bare = await host.Client.PostAsJsonAsync(QuillRoutes.AuthLogin, new { apiKey = secondSecret });
        Assert.Equal(HttpStatusCode.Unauthorized, bare.StatusCode);

        var primarySecret = await host.Client.PostAsJsonAsync(QuillRoutes.AuthLogin,
            new { apiKey = "second/" + ApplianceWebApplicationFactory.TestApiKey });
        Assert.Equal(HttpStatusCode.Unauthorized, primarySecret.StatusCode);

        using var client = host.Factory.CreateClient();
        client.DefaultRequestHeaders.Remove(ApiKeyAuthenticationHandler.HeaderName);
        client.DefaultRequestHeaders.Add(ApiKeyAuthenticationHandler.HeaderName, "second/" + secondSecret);
        var apps = await client.GetAsync(QuillRoutes.Apps);
        Assert.True(apps.IsSuccessStatusCode, await apps.Content.ReadAsStringAsync());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Primary_key_is_reseeded_from_the_environment_on_startup()
    {
        await using var host = await NewHostAsync();
        host.Client.DefaultRequestHeaders.Remove(ApiKeyAuthenticationHandler.HeaderName);

        var login = await host.Client.PostAsJsonAsync(QuillRoutes.AuthLogin,
            new { apiKey = ApplianceWebApplicationFactory.TestApiKey });
        Assert.True(login.IsSuccessStatusCode, await login.Content.ReadAsStringAsync());

        using var session = host.Config.OpenAsyncSession();
        var doc = await session.LoadAsync<ApiKey>(ApiKey.PrimaryId);
        Assert.NotNull(doc);
        Assert.False(doc.Revoked);
        var salt = Convert.FromBase64String(doc.Salt);
        Assert.Equal(ApiKeyStore.HashSecret(salt, ApplianceWebApplicationFactory.TestApiKey), Convert.FromBase64String(doc.Hash));
    }

    private static async Task SetPrimaryRevokedAsync(QuillHost host, bool revoked)
    {
        using var session = host.Config.OpenAsyncSession();
        var doc = await session.LoadAsync<ApiKey>(ApiKey.PrimaryId);
        Assert.NotNull(doc);
        doc.Revoked = revoked;
        await session.SaveChangesAsync();
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Key_id_prefix_on_the_configured_key_is_ignored()
    {
        await using var host = await NewHostAsync(configure: opts =>
            opts.ApiKey = "primary/" + ApplianceWebApplicationFactory.TestApiKey);
        host.Client.DefaultRequestHeaders.Remove(ApiKeyAuthenticationHandler.HeaderName);

        var bare = await host.Client.PostAsJsonAsync(QuillRoutes.AuthLogin,
            new { apiKey = ApplianceWebApplicationFactory.TestApiKey });
        Assert.True(bare.IsSuccessStatusCode, await bare.Content.ReadAsStringAsync());

        var prefixed = await host.Client.PostAsJsonAsync(QuillRoutes.AuthLogin,
            new { apiKey = "primary/" + ApplianceWebApplicationFactory.TestApiKey });
        Assert.True(prefixed.IsSuccessStatusCode, await prefixed.Content.ReadAsStringAsync());
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Key_id_prefix_with_wrong_secret_is_401()
    {
        await using var host = await NewHostAsync();
        host.Client.DefaultRequestHeaders.Remove(ApiKeyAuthenticationHandler.HeaderName);

        var wrongSecret = await host.Client.PostAsJsonAsync(QuillRoutes.AuthLogin, new { apiKey = "primary/wrong-key" });
        Assert.Equal(HttpStatusCode.Unauthorized, wrongSecret.StatusCode);

        var emptySecret = await host.Client.PostAsJsonAsync(QuillRoutes.AuthLogin, new { apiKey = "primary/" });
        Assert.Equal(HttpStatusCode.Unauthorized, emptySecret.StatusCode);
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
