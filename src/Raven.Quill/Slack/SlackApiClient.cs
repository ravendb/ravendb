using SlackNet;
using SlackNet.Blocks;
using SlackNet.WebApi;

namespace Raven.Quill.Slack;

internal sealed class SlackApiClient(SlackSdk sdk) : ISlackClient
{
    internal const int MaxEscapeExpansion = 5;

    public async Task<SlackAuthInfo> AuthTestAsync(string botToken, CancellationToken ct)
    {
        var payload = await CallAsync(() => Api(botToken).Auth.Test(ct), "auth.test", ct);
        return new SlackAuthInfo(payload.TeamId, payload.Team ?? "", payload.UserId, payload.User ?? "");
    }

    public async Task<string> OpenSocketAsync(string appToken, CancellationToken ct)
    {
        var payload = await CallAsync(
            () => Api(appToken).AppsConnectionsApi.Open(ct), "apps.connections.open", ct);
        return payload.Url;
    }

    public async Task<string> PostMessageAsync(
        string botToken, string channel, string markdown, CancellationToken ct)
    {
        var message = new Message
        {
            Channel = channel,
            Text = Escape(markdown),
            Parse = ParseMode.None,
            Blocks = [new MarkdownBlock { Text = markdown }],
        };
        var payload = await CallAsync(() => Api(botToken).Chat.PostMessage(message, ct), "chat.postMessage", ct);
        return payload.Ts;
    }

    public Task UpdateMessageAsync(
        string botToken, string channel, string ts, string markdown, CancellationToken ct)
    {
        var update = new MessageUpdate
        {
            ChannelId = channel,
            Ts = ts,
            Text = Escape(markdown),
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

    internal static string Escape(string text) =>
        string.IsNullOrEmpty(text)
            ? text
            : text.Replace("&", "&amp;").Replace("<", "&lt;").Replace(">", "&gt;");

    private ISlackApiClient Api(string token) => sdk.Api.WithAccessToken(token);

    private static async Task<T> CallAsync<T>(Func<Task<T>> call, string method, CancellationToken ct)
    {
        try
        {
            return await call();
        }
        catch (Exception e) when (ct.IsCancellationRequested == false)
        {
            throw SlackApiErrors.Translate(e, method) ?? e;
        }
    }
}
