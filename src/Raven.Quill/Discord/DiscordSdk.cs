using System.Net;
using System.Net.Http.Headers;
using System.Text.Json;
using Discord;
using Discord.Net;
using Discord.Net.Rest;
using Discord.Rest;
using Discord.WebSocket;
using Microsoft.Extensions.Options;
using Raven.Quill.Hosting;

namespace Raven.Quill.Discord;

internal sealed record DiscordCurrentUser(string Id, string? Username, bool IsBot);

internal sealed class DiscordSdk(IHttpClientFactory httpFactory, IOptions<ApplianceOptions> options)
{
    internal const string HttpClientName = "discord";

    private readonly DiscordOptions _options = options.Value.Discord;

    public TimeSpan RequestTimeout => _options.RequestTimeout;

    public DiscordRestClient NewRestClient() => new(Configure(new DiscordRestConfig()));

    public DiscordSocketClient NewSocketClient() => new(Configure(new DiscordSocketConfig
    {
        GatewayIntents = GatewayIntents.DirectMessages,
        ConnectionTimeout = (int)_options.GatewayHandshakeTimeout.TotalMilliseconds,
        LogGatewayIntentWarnings = false,
    }));

    public RequestOptions NewRequestOptions(CancellationToken ct) => new()
    {
        CancelToken = ct,
        Timeout = (int)_options.RequestTimeout.TotalMilliseconds,
    };

    public async Task<DiscordCurrentUser> GetCurrentUserAsync(string botToken, CancellationToken ct)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, ApiUrl + "users/@me");
        request.Headers.Authorization = new AuthenticationHeaderValue("Bot", botToken);

        using var response = await httpFactory.CreateClient(HttpClientName).SendAsync(request, ct);
        if (response.StatusCode == HttpStatusCode.TooManyRequests)
            throw new RateLimitedException(null);

        var body = await response.Content.ReadAsStringAsync(ct);
        if (response.IsSuccessStatusCode == false)
            throw new HttpException(response.StatusCode, null, reason: ReasonOf(body));

        using var payload = JsonDocument.Parse(body);
        var user = payload.RootElement;
        return new DiscordCurrentUser(
            user.TryGetProperty("id", out var id) ? id.GetString() ?? "" : "",
            user.TryGetProperty("username", out var username) ? username.GetString() : null,
            user.TryGetProperty("bot", out var bot) && bot.ValueKind == JsonValueKind.True);
    }

    private string ApiUrl => _options.ApiUrl.TrimEnd('/') + "/";

    private static string? ReasonOf(string body)
    {
        try
        {
            using var payload = JsonDocument.Parse(body);
            return payload.RootElement.TryGetProperty("message", out var message) ? message.GetString() : null;
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private TConfig Configure<TConfig>(TConfig config) where TConfig : DiscordRestConfig
    {
        var apiUrl = ApiUrl;
        config.RestClientProvider = _ => DefaultRestClientProvider.Instance(apiUrl);
        config.DefaultRetryMode = RetryMode.RetryRatelimit | RetryMode.Retry502;
        config.LogLevel = LogSeverity.Info;
        return config;
    }
}
