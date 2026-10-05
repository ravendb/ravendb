using SlackNet;
using SlackNet.Blocks;
using SlackNet.WebApi;

namespace Raven.Quill.Slack;

internal sealed class SlackApiClient(SlackSdk sdk) : ISlackClient
{
    internal const int MaxEscapeExpansion = 5;

    public async Task<SlackAuthInfo> AuthTestAsync(string botToken, CancellationToken ct)
    {
        var payload = await Api(botToken).Auth.Test(ct);
        return new SlackAuthInfo(payload.TeamId, payload.Team ?? "", payload.UserId, payload.User ?? "");
    }

    public async Task<string> OpenSocketAsync(string appToken, CancellationToken ct)
    {
        var payload = await Api(appToken).AppsConnectionsApi.Open(ct);
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
        var payload = await Api(botToken).Chat.PostMessage(message, ct);
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
        return Api(botToken).Chat.Update(update, ct);
    }

    public async Task<SlackUserInfo> UserInfoAsync(string botToken, string userId, CancellationToken ct)
    {
        var user = await Api(botToken).Users.Info(userId, cancellationToken: ct);

        var email = user?.Profile?.Email;
        return new SlackUserInfo(userId, string.IsNullOrWhiteSpace(email) ? null : email);
    }

    internal static string Escape(string text) =>
        string.IsNullOrEmpty(text)
            ? text
            : text.Replace("&", "&amp;").Replace("<", "&lt;").Replace(">", "&gt;");

    private ISlackApiClient Api(string token) => sdk.Api.WithAccessToken(token);
}
