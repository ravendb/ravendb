using System.Net;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Raven.Quill.Slack;

internal sealed class SlackApiClient(HttpClient http) : ISlackClient
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    public async Task<(SlackAuthInfo? Info, string? Error, bool SlackResponded)> AuthTestAsync(
        string botToken, CancellationToken ct)
    {
        try
        {
            using var request = NewRequest("auth.test", botToken);
            using var response = await http.SendAsync(request, ct);
            var body = await response.Content.ReadAsStringAsync(ct);

            if (response.StatusCode == HttpStatusCode.TooManyRequests)
                return (null, "slack is rate-limiting the token check; try again shortly", false);

            if ((int)response.StatusCode >= 500)
                return (null, $"the Slack API is unavailable (status {(int)response.StatusCode})", false);

            var payload = Deserialize<AuthTestResponse>(body);
            if (payload is null)
                return (null, "slack returned an unrecognized auth.test payload", true);

            if (payload.Ok == false)
            {
                if (payload.Error == SlackApiException.RateLimitedError)
                    return (null, "slack is rate-limiting the token check; try again shortly", false);

                return (null, payload.Error is "invalid_auth" or "token_revoked" or "account_inactive"
                    ? "slack rejected the bot token; copy the xoxb- token from the app's OAuth page and try again"
                    : $"slack refused the token check: {payload.Error ?? "unknown error"}", true);
            }

            if (string.IsNullOrEmpty(payload.TeamId) || string.IsNullOrEmpty(payload.UserId))
                return (null, "slack returned an unrecognized auth.test payload", true);

            return (new SlackAuthInfo(
                payload.TeamId,
                payload.Team ?? "",
                payload.UserId,
                payload.BotId,
                payload.User ?? "",
                payload.Url), null, true);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested == false)
        {
            return (null, "the Slack API did not respond while validating the bot token", false);
        }
        catch (HttpRequestException e)
        {
            return (null, $"could not reach the Slack API: {e.Message}", false);
        }
    }

    public async Task<string> PostMessageAsync(
        string botToken, string channel, string text, CancellationToken ct)
    {
        var payload = await SendAsync("chat.postMessage", botToken, new { channel, text }, ct);
        if (string.IsNullOrEmpty(payload.Ts))
            throw new SlackApiException("slack returned a chat.postMessage payload without a message ts");

        return payload.Ts;
    }

    public async Task UpdateMessageAsync(
        string botToken, string channel, string ts, string text, CancellationToken ct)
    {
        await SendAsync("chat.update", botToken, new { channel, ts, text }, ct);
    }

    private async Task<ApiResponse> SendAsync(string method, string botToken, object body, CancellationToken ct)
    {
        using var request = NewRequest(method, botToken);
        request.Content = JsonContent.Create(body);

        HttpResponseMessage response;
        try
        {
            response = await http.SendAsync(request, ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested == false)
        {
            throw new SlackApiException($"the Slack API did not respond to {method}");
        }
        catch (HttpRequestException e)
        {
            throw new SlackApiException($"could not reach the Slack API: {e.Message}", inner: e);
        }

        using (response)
        {
            var raw = await response.Content.ReadAsStringAsync(ct);

            if (response.StatusCode == HttpStatusCode.TooManyRequests)
                throw new SlackApiException($"slack rate-limited {method}",
                    SlackApiException.RateLimitedError, response.Headers.RetryAfter?.Delta);

            if ((int)response.StatusCode >= 500)
                throw new SlackApiException($"the Slack API is unavailable (status {(int)response.StatusCode})");

            var payload = Deserialize<ApiResponse>(raw);
            if (payload is null)
                throw new SlackApiException($"slack returned an unrecognized {method} payload");

            if (payload.Ok == false)
                throw new SlackApiException(
                    $"slack refused {method}: {payload.Error ?? "unknown error"}",
                    payload.Error,
                    payload.Error == SlackApiException.RateLimitedError ? response.Headers.RetryAfter?.Delta : null);

            return payload;
        }
    }

    private static HttpRequestMessage NewRequest(string method, string botToken)
    {
        var request = new HttpRequestMessage(HttpMethod.Post, method);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", botToken);
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

    private sealed class ApiResponse
    {
        [JsonPropertyName("ok")]
        public bool Ok { get; set; }

        [JsonPropertyName("error")]
        public string? Error { get; set; }

        [JsonPropertyName("ts")]
        public string? Ts { get; set; }
    }

    private sealed class AuthTestResponse
    {
        [JsonPropertyName("ok")]
        public bool Ok { get; set; }

        [JsonPropertyName("error")]
        public string? Error { get; set; }

        [JsonPropertyName("team_id")]
        public string? TeamId { get; set; }

        [JsonPropertyName("team")]
        public string? Team { get; set; }

        [JsonPropertyName("user_id")]
        public string? UserId { get; set; }

        [JsonPropertyName("bot_id")]
        public string? BotId { get; set; }

        [JsonPropertyName("user")]
        public string? User { get; set; }

        [JsonPropertyName("url")]
        public string? Url { get; set; }
    }
}
