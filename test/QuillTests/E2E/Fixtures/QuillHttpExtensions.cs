using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace QuillTests.E2E.Fixtures;

/// Thrown by the wrapper methods on a non-2xx; negative-path tests assert on it via Assert.ThrowsAsync.
internal sealed class QuillHttpException(HttpStatusCode statusCode, string message, string body) : Exception(message)
{
    public HttpStatusCode StatusCode { get; } = statusCode;
    public string Body { get; } = body;
}

/// Shared HTTP plumbing for the wrapper methods: JSON policy, typed send helpers, success/read extensions.
internal static class QuillHttp
{
    /// Web defaults (camelCase) + string enums; used for both serialize and deserialize.
    internal static readonly JsonSerializerOptions Json =
        new(JsonSerializerDefaults.Web) { Converters = { new JsonStringEnumConverter() } };

    public static async Task<T> PostAsync<T>(HttpClient client, string route, object body,
        Action<HttpRequestMessage>? configureRequest = null, CancellationToken ct = default)
    {
        using var resp = await SendAsync(client, HttpMethod.Post, route, body, configureRequest, ct);
        return await ReadBodyAsync<T>(resp);
    }

    public static async Task<T> PutAsync<T>(HttpClient client, string route, object body,
        Action<HttpRequestMessage>? configureRequest = null, CancellationToken ct = default)
    {
        using var resp = await SendAsync(client, HttpMethod.Put, route, body, configureRequest, ct);
        return await ReadBodyAsync<T>(resp);
    }

    public static async Task<T> GetAsync<T>(HttpClient client, string route,
        Action<HttpRequestMessage>? configureRequest = null, CancellationToken ct = default)
    {
        using var resp = await SendAsync(client, HttpMethod.Get, route, configureRequest: configureRequest, ct: ct);
        return await ReadBodyAsync<T>(resp);
    }

    public static async Task DeleteAsync(HttpClient client, string route,
        Action<HttpRequestMessage>? configureRequest = null, CancellationToken ct = default)
    {
        using var resp = await SendAsync(client, HttpMethod.Delete, route, configureRequest: configureRequest, ct: ct);
    }

    /// A non-2xx throws <see cref="QuillHttpException"/> (response disposed before throwing); on success the
    /// caller reads and disposes the returned response.
    public static async Task<HttpResponseMessage> SendAsync(
        HttpClient client, HttpMethod method, string route, object? body = null,
        Action<HttpRequestMessage>? configureRequest = null, CancellationToken ct = default)
    {
        using var req = new HttpRequestMessage(method, route);
        if (body is not null)
            req.Content = JsonContent.Create(body, options: Json);
        configureRequest?.Invoke(req);

        var resp = await client.SendAsync(req, ct);
        try
        {
            await resp.EnsureSuccessAsync();
        }
        catch
        {
            resp.Dispose();
            throw;
        }

        return resp;
    }

    /// <c>T = string</c> returns the raw content (HTML page / NDJSON stream); any other T is JSON.
    private static async Task<T> ReadBodyAsync<T>(HttpResponseMessage resp)
    {
        if (typeof(T) == typeof(string))
            return (T)(object)await resp.Content.ReadAsStringAsync();

        return (await resp.Content.ReadFromJsonAsync<T>(Json))!;
    }
}

internal static class QuillHttpExtensions
{
    /// On a non-2xx, throws <see cref="QuillHttpException"/> carrying the status + body.
    public static async Task<HttpResponseMessage> EnsureSuccessAsync(this HttpResponseMessage resp)
    {
        if (resp.IsSuccessStatusCode)
            return resp;

        var body = await resp.Content.ReadAsStringAsync();
        throw new QuillHttpException(resp.StatusCode,
            $"{resp.RequestMessage?.Method} {resp.RequestMessage?.RequestUri} → {(int)resp.StatusCode} {resp.StatusCode}: {body}",
            body);
    }
}

/// Every Quill route template in one place — reused by the wrapper methods and negative-path tests.
internal static class QuillRoutes
{
    // server-wide
    public const string Apps = "/api/apps";
    public static string App(string slug) => $"{Apps}/{slug}";
    public const string AuthLogin = "/api/auth/login";
    public const string AuthStatus = "/api/auth/status";
    public const string ConnectionStrings = "/api/ai/connection-strings";
    public const string ConnectionStringsTest = $"{ConnectionStrings}/test";
    public static string ConnectionString(string name) => $"{ConnectionStrings}/{name}";
    public const string AiModels = "/api/ai/models";
    public const string SettingsLicense = "/api/settings/license";
    public const string SettingsUsage = "/api/settings/usage";

    // wizard (config-DB scoped)
    public const string SetupConnect = "/api/setup/connect";
    public const string SetupDiscover = "/api/setup/discover";
    public const string SetupMap = "/api/setup/map";
    public const string SetupVerifyCdc = "/api/setup/verify-cdc";
    public const string SetupProvision = "/api/setup/provision";
    public const string SetupTestMapping = "/api/setup/test-mapping";
    public const string SuggestCdc = "/api/setup/suggest/cdc";

    // global (config-DB fan-out)
    public const string Usage = "/api/usage";
    public const string UsageByApp = "/api/usage/by-app";
    public const string DashboardApps = "/api/dashboard/apps";
    public static string DashboardApp(string slug) => $"/api/dashboard/apps/{slug}";

    // app-scoped
    public static string AppConnectionStrings(string slug) => $"/api/apps/{slug}/connection-strings";
    public static string AppUsage(string slug) => $"/api/apps/{slug}/usage";
    public static string AppOverview(string slug) => $"/api/apps/{slug}/overview";
    public static string AppCollections(string slug) => $"/api/apps/{slug}/collections";
    public static string AppActivity(string slug) => $"/api/apps/{slug}/activity";
    public static string AppChannelStats(string slug) => $"/api/apps/{slug}/channels/stats";
    public static string AppConversations(string slug) => $"/api/apps/{slug}/conversations";
    public static string AppConversationStats(string slug) => $"/api/apps/{slug}/conversations/stats";
    public static string WidgetTheme(string slug, string channelId) => $"/api/apps/{slug}/iframe/{channelId}/theme";
    public static string WidgetDefaultTheme(string slug) => $"/api/apps/{slug}/iframe/default-theme";
    public static string EmbedPage(string slug, string token) => $"/apps/{slug}/embed/{token}";
    public static string EmbedChat(string slug, string token) => $"{EmbedPage(slug, token)}/chat";
    public static string Agents(string slug) => $"/api/apps/{slug}/agents";
    public static string Agent(string slug, string agentId) => $"/api/apps/{slug}/agent/{agentId}";
    public static string SetupAgent(string slug) => $"/api/apps/{slug}/setup/agent";
    public static string EditAgent(string slug) => $"/api/apps/{slug}/agent";
    public static string SuggestAgent(string slug) => $"/api/apps/{slug}/suggest/agent";

    public static string Channels(string slug) => $"/api/apps/{slug}/channels";
    public static string Channel(string slug, string channelId) => $"/api/apps/{slug}/channels/{channelId}";
    public static string SetupChannel(string slug) => $"/api/apps/{slug}/setup/channel";
    public static string SetupTry(string slug) => $"/api/apps/{slug}/setup/try";
    public static string CdcProgress(string slug) => $"/api/apps/{slug}/cdc/progress";

    public static string AppCdc(string slug) => $"/api/apps/{slug}/cdc";
    public static string AppCdcPerformance(string slug) => $"/api/apps/{slug}/cdc/performance";
    public static string AppCdcErrors(string slug) => $"/api/apps/{slug}/cdc/errors";

    public static string EmbedLinks(string slug) => $"/api/apps/{slug}/embed-links";
    public static string EmbedLink(string slug, string token) => $"/api/apps/{slug}/embed-links/{token}";

    public static string SlackWebhook(string token) => $"/webhooks/slack/{token}";
    public static string SlackWebhookInfo(string slug, string channelId) =>
        $"/api/apps/{slug}/channels/{channelId}/slack/webhook";
    public static string SlackHealth(string slug) => $"/api/apps/{slug}/slack/health";

    public static string DiscordHealth(string slug) => $"/api/apps/{slug}/discord/health";
}

/// The <c>year[&amp;month[&amp;day]]</c> query shared by the usage/stats endpoints.
internal static class Periods
{
    public static string Query(int year, int? month, int? day)
    {
        var q = $"year={year}";
        if (month is not null) q += $"&month={month}";
        if (day is not null) q += $"&day={day}";
        return q;
    }
}
