using SlackNet;
using SlackNet.Blocks;
using SlackNet.WebApi;

namespace Raven.Quill.Slack;

internal sealed class SlackApiClient(SlackSdk sdk) : ISlackClient
{
    public async Task<(SlackAuthInfo? Info, string? Error, bool SlackResponded)> AuthTestAsync(
        string botToken, CancellationToken ct)
    {
        try
        {
            var payload = await CallAsync(() => Api(botToken).Auth.Test(ct), "auth.test", ct);

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
            () => Api(appToken).AppsConnectionsApi.Open(ct), "apps.connections.open", ct);

        if (string.IsNullOrEmpty(payload?.Url) ||
            Uri.TryCreate(payload.Url, UriKind.Absolute, out var url) == false ||
            url.Scheme is not ("wss" or "ws"))
            throw new SlackApiException(
                "slack returned an apps.connections.open payload without a websocket url", slackResponded: true);

        return payload.Url;
    }

    public async Task<string> PostMessageAsync(
        string botToken, string channel, string markdown, CancellationToken ct)
    {
        var message = new Message
        {
            Channel = channel,
            Text = SlackText.Escape(markdown),
            Parse = ParseMode.None,
            Blocks = [new MarkdownBlock { Text = markdown }],
        };
        var payload = await CallAsync(() => Api(botToken).Chat.PostMessage(message, ct), "chat.postMessage", ct);

        if (string.IsNullOrEmpty(payload?.Ts))
            throw new SlackApiException("slack returned a chat.postMessage payload without a message ts");

        return payload.Ts;
    }

    public Task UpdateMessageAsync(
        string botToken, string channel, string ts, string markdown, CancellationToken ct)
    {
        var update = new MessageUpdate
        {
            ChannelId = channel,
            Ts = ts,
            Text = SlackText.Escape(markdown),
            Parse = ParseMode.None,
            Blocks = [new MarkdownBlock { Text = markdown }],
        };
        return CallAsync(() => Api(botToken).Chat.Update(update, ct), "chat.update", ct);
    }

    public async Task<SlackUserInfo> UserInfoAsync(string botToken, string userId, CancellationToken ct)
    {
        var user = await CallAsync(
            () => Api(botToken).Users.Info(userId, cancellationToken: ct), "users.info", ct);

        var email = user?.Profile?.Email;
        return new SlackUserInfo(userId, string.IsNullOrWhiteSpace(email) ? null : email);
    }

    private ISlackApiClient Api(string token) => sdk.Api.WithAccessToken(token);

    private static async Task<T> CallAsync<T>(Func<Task<T>> call, string method, CancellationToken ct)
    {
        try
        {
            return await call();
        }
        catch (Exception e) when (ct.IsCancellationRequested == false)
        {
            var translated = SlackApiErrors.Translate(e, method);
            if (translated is null)
                throw;

            throw translated;
        }
    }
}
