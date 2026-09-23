using System.Collections;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Raven.Quill.Hosting;
using SlackNet;
using SlackNet.WebApi;
using SdkClient = SlackNet.SlackApiClient;

namespace Raven.Quill.Slack;

internal sealed class SlackApiClient : ISlackClient
{
    internal static readonly SlackJsonSettings JsonSettings = Default.JsonSettings();

    private readonly ISlackApiClient _sdk;

    public SlackApiClient(HttpClient http, IOptions<ApplianceOptions> options)
    {
        _sdk = new SdkClient(
            Default.Http(JsonSettings, () => http),
            new SlackUrlBuilder(options.Value.Slack.ApiUrl, JsonSettings),
            JsonSettings,
            token: "")
        {
            DisableRetryOnRateLimit = true,
        };
    }

    public async Task<(SlackAuthInfo? Info, string? Error, bool SlackResponded)> AuthTestAsync(
        string botToken, CancellationToken ct)
    {
        try
        {
            var payload = await CallAsync(() => _sdk.WithAccessToken(botToken).Auth.Test(ct), "auth.test", ct);

            if (string.IsNullOrEmpty(payload?.TeamId) || string.IsNullOrEmpty(payload.UserId))
                return (null, "slack returned an unrecognized auth.test payload", true);

            return (new SlackAuthInfo(
                payload.TeamId,
                payload.Team ?? "",
                payload.UserId,
                payload.User ?? ""), null, true);
        }
        catch (SlackApiException e)
        {
            if (e.Error == SlackApiException.RateLimitedError)
                return (null, "slack is rate-limiting the token check; try again shortly", false);

            if (e.SlackResponded == false || e.Error is null)
                return (null, e.Message, e.SlackResponded);

            return (null, e.Error is "invalid_auth" or "token_revoked" or "account_inactive"
                ? "slack rejected the bot token; copy the xoxb- token from the app's OAuth page and try again"
                : $"slack refused the token check: {e.Error}", true);
        }
    }

    public async Task<string> OpenSocketAsync(string appToken, CancellationToken ct)
    {
        var payload = await CallAsync(
            () => _sdk.WithAccessToken(appToken).AppsConnectionsApi.Open(ct), "apps.connections.open", ct);

        if (string.IsNullOrEmpty(payload?.Url) ||
            Uri.TryCreate(payload.Url, UriKind.Absolute, out var url) == false ||
            url.Scheme is not ("wss" or "ws"))
            throw new SlackApiException(
                "slack returned an apps.connections.open payload without a websocket url", slackResponded: true);

        return payload.Url;
    }

    public async Task<string> PostMessageAsync(
        string botToken, string channel, string text, CancellationToken ct)
    {
        var message = new Message { Channel = channel, Text = text, Parse = ParseMode.None };
        var payload = await CallAsync(
            () => _sdk.WithAccessToken(botToken).Chat.PostMessage(message, ct), "chat.postMessage", ct);

        if (string.IsNullOrEmpty(payload?.Ts))
            throw new SlackApiException("slack returned a chat.postMessage payload without a message ts");

        return payload.Ts;
    }

    public Task UpdateMessageAsync(
        string botToken, string channel, string ts, string text, CancellationToken ct)
    {
        var update = new MessageUpdate { ChannelId = channel, Ts = ts, Text = text, Parse = ParseMode.None };
        return CallAsync(() => _sdk.WithAccessToken(botToken).Chat.Update(update, ct), "chat.update", ct);
    }

    public async Task<SlackUserInfo> UserInfoAsync(string botToken, string userId, CancellationToken ct)
    {
        var user = await CallAsync(
            () => _sdk.WithAccessToken(botToken).Users.Info(userId, cancellationToken: ct), "users.info", ct);

        var email = user?.Profile?.Email;
        return new SlackUserInfo(userId, string.IsNullOrWhiteSpace(email) ? null : email);
    }

    private static async Task<T> CallAsync<T>(Func<Task<T>> call, string method, CancellationToken ct)
    {
        try
        {
            return await call();
        }
        catch (SlackRateLimitException e)
        {
            throw new SlackApiException(
                $"slack rate-limited {method}", SlackApiException.RateLimitedError, e.RetryAfter, e,
                slackResponded: true);
        }
        catch (SlackException e)
        {
            throw new SlackApiException($"slack refused {method}: {e.ErrorCode}", e.ErrorCode, inner: e,
                slackResponded: true);
        }
        catch (HttpRequestException e) when (e.StatusCode is { } status && (int)status >= 500)
        {
            throw new SlackApiException($"the Slack API is unavailable (status {(int)status})", inner: e);
        }
        catch (HttpRequestException e)
        {
            throw new SlackApiException($"could not reach the Slack API: {e.Message}", inner: e);
        }
        catch (JsonException e)
        {
            throw new SlackApiException($"slack returned an unrecognized {method} payload", inner: e,
                slackResponded: true);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested == false)
        {
            throw new SlackApiException($"the Slack API did not respond to {method}");
        }
    }

    private sealed class SlackUrlBuilder(string apiUrl, SlackJsonSettings json) : ISlackUrlBuilder
    {
        private readonly string _base = apiUrl.TrimEnd('/') + "/";

        public string Url(string apiMethod, Dictionary<string, object> args)
        {
            var query = string.Join("&", args
                .Where(a => a.Value is not null)
                .Select(a => $"{a.Key}={Uri.EscapeDataString(ValueOf(a.Value))}"));

            return query.Length == 0 ? _base + apiMethod : $"{_base}{apiMethod}?{query}";
        }

        private string ValueOf(object value) => value switch
        {
            string s => s,
            IDictionary d => Serialize(d),
            IEnumerable e => string.Join(",", e.Cast<object>().Select(Serialize)),
            _ => Serialize(value),
        };

        private string Serialize(object value) =>
            JsonConvert.SerializeObject(value, json.SerializerSettings).Trim('"');
    }
}
