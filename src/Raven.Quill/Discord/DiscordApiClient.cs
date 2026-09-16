using System.Net;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Raven.Quill.Discord;

internal sealed class DiscordApiClient(HttpClient http) : IDiscordClient
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    public async Task<(DiscordBotIdentity? Identity, string? Error, bool DiscordResponded)> GetBotIdentityAsync(
        string botToken, CancellationToken ct)
    {
        var (user, userError, userResponded) = await TryGetAsync<CurrentUserResponse>("users/@me", botToken, ct);
        if (user is null)
            return (null, userError, userResponded);

        if (string.IsNullOrEmpty(user.Id) || string.IsNullOrEmpty(user.Username))
            return (null, "discord returned an unrecognized users/@me payload", true);

        if (user.Bot != true)
            return (null, "that token belongs to a user account, not a bot; copy the token from the app's Bot page", true);

        var (application, applicationError, applicationResponded) =
            await TryGetAsync<CurrentApplicationResponse>("oauth2/applications/@me", botToken, ct);
        if (application is null)
            return (null, applicationError, applicationResponded);

        if (string.IsNullOrEmpty(application.Id))
            return (null, "discord returned an unrecognized oauth2/applications/@me payload", true);

        return (new DiscordBotIdentity(application.Id, user.Id, user.Username), null, true);
    }

    public async Task<string> GetGatewayUrlAsync(string botToken, CancellationToken ct)
    {
        using var request = NewRequest(HttpMethod.Get, "gateway/bot", botToken);
        var payload = await SendAsync<GatewayResponse>(request, "gateway/bot", ct);

        if (string.IsNullOrEmpty(payload.Url))
            throw new DiscordApiException("discord returned a gateway/bot payload without a url");

        return payload.Url;
    }

    public async Task<string> CreateMessageAsync(
        string botToken, string channelId, string content, CancellationToken ct)
    {
        using var request = NewRequest(HttpMethod.Post, $"channels/{channelId}/messages", botToken);
        request.Content = JsonContent.Create(new MessageRequest(content));

        var payload = await SendAsync<MessageResponse>(request, "create message", ct);
        if (string.IsNullOrEmpty(payload.Id))
            throw new DiscordApiException("discord returned a create message payload without an id");

        return payload.Id;
    }

    public async Task EditMessageAsync(
        string botToken, string channelId, string messageId, string content, CancellationToken ct)
    {
        using var request = NewRequest(HttpMethod.Patch, $"channels/{channelId}/messages/{messageId}", botToken);
        request.Content = JsonContent.Create(new MessageRequest(content));

        await SendAsync<MessageResponse>(request, "edit message", ct);
    }

    private async Task<(TResponse? Payload, string? Error, bool DiscordResponded)> TryGetAsync<TResponse>(
        string path, string botToken, CancellationToken ct)
        where TResponse : class
    {
        try
        {
            using var request = NewRequest(HttpMethod.Get, path, botToken);
            using var response = await http.SendAsync(request, ct);
            var raw = await response.Content.ReadAsStringAsync(ct);

            if (response.StatusCode == HttpStatusCode.TooManyRequests)
                return (null, "discord is rate-limiting the token check; try again shortly", false);

            if ((int)response.StatusCode >= 500)
                return (null, $"the Discord API is unavailable (status {(int)response.StatusCode})", false);

            if (response.StatusCode is HttpStatusCode.Unauthorized or HttpStatusCode.Forbidden)
                return (null, "discord rejected the bot token; reset it on the app's Bot page and try again", true);

            if (response.IsSuccessStatusCode == false)
                return (null, $"discord refused {path}: {ErrorTextOf(raw, response.StatusCode)}", true);

            var payload = Deserialize<TResponse>(raw);
            return payload is null
                ? (null, $"discord returned an unrecognized {path} payload", true)
                : (payload, null, true);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested == false)
        {
            return (null, "the Discord API did not respond while validating the bot token", false);
        }
        catch (HttpRequestException e)
        {
            return (null, $"could not reach the Discord API: {e.Message}", false);
        }
    }

    private async Task<TResponse> SendAsync<TResponse>(
        HttpRequestMessage request, string what, CancellationToken ct)
        where TResponse : class
    {
        HttpResponseMessage response;
        try
        {
            response = await http.SendAsync(request, ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested == false)
        {
            throw new DiscordApiException($"the Discord API did not respond to {what}");
        }
        catch (HttpRequestException e)
        {
            throw new DiscordApiException($"could not reach the Discord API: {e.Message}", inner: e);
        }

        using (response)
        {
            var raw = await response.Content.ReadAsStringAsync(ct);

            if (response.StatusCode == HttpStatusCode.TooManyRequests)
                throw new DiscordApiException(
                    $"discord rate-limited {what}", response.StatusCode, RetryAfterOf(raw, response));

            if (response.IsSuccessStatusCode == false)
                throw new DiscordApiException(
                    $"discord refused {what}: {ErrorTextOf(raw, response.StatusCode)}", response.StatusCode);

            var payload = Deserialize<TResponse>(raw);
            if (payload is null)
                throw new DiscordApiException($"discord returned an unrecognized {what} payload");

            return payload;
        }
    }

    private static TimeSpan? RetryAfterOf(string raw, HttpResponseMessage response)
    {
        var seconds = Deserialize<RateLimitResponse>(raw)?.RetryAfter;
        if (seconds is > 0)
            return TimeSpan.FromSeconds(seconds.Value);

        return response.Headers.RetryAfter?.Delta;
    }

    private static string ErrorTextOf(string raw, HttpStatusCode status)
    {
        var message = Deserialize<ApiErrorPayload>(raw)?.Message;
        return string.IsNullOrWhiteSpace(message) ? $"status {(int)status}" : message;
    }

    private static HttpRequestMessage NewRequest(HttpMethod verb, string pathAndQuery, string botToken)
    {
        var request = new HttpRequestMessage(verb, pathAndQuery);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bot", botToken);
        return request;
    }

    private static T? Deserialize<T>(string body) where T : class
    {
        if (string.IsNullOrWhiteSpace(body))
            return null;

        try
        {
            return JsonSerializer.Deserialize<T>(body, JsonOptions);
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private sealed record MessageRequest(
        [property: JsonPropertyName("content")] string Content)
    {
        [JsonPropertyName("allowed_mentions")]
        public AllowedMentions AllowedMentions { get; } = AllowedMentions.None;
    }

    private sealed record AllowedMentions(
        [property: JsonPropertyName("parse")] string[] Parse)
    {
        internal static readonly AllowedMentions None = new([]);
    }

    private sealed class MessageResponse
    {
        [JsonPropertyName("id")]
        public string? Id { get; set; }
    }

    private sealed class CurrentUserResponse
    {
        [JsonPropertyName("id")]
        public string? Id { get; set; }

        [JsonPropertyName("username")]
        public string? Username { get; set; }

        [JsonPropertyName("bot")]
        public bool? Bot { get; set; }
    }

    private sealed class CurrentApplicationResponse
    {
        [JsonPropertyName("id")]
        public string? Id { get; set; }
    }

    private sealed class GatewayResponse
    {
        [JsonPropertyName("url")]
        public string? Url { get; set; }
    }

    private sealed class RateLimitResponse
    {
        [JsonPropertyName("retry_after")]
        public double? RetryAfter { get; set; }
    }

    private sealed class ApiErrorPayload
    {
        [JsonPropertyName("message")]
        public string? Message { get; set; }
    }
}
