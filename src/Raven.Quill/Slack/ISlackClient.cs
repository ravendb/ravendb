namespace Raven.Quill.Slack;

internal sealed record SlackAuthInfo(
    string TeamId,
    string TeamName,
    string BotUserId,
    string BotName);

internal sealed record SlackUserInfo(
    string UserId,
    string? Email);

internal interface ISlackClient
{
    Task<SlackAuthInfo> AuthTestAsync(string botToken, CancellationToken ct);

    Task<string> OpenSocketAsync(string appToken, CancellationToken ct);

    Task<string> PostMessageAsync(
        string botToken, string channel, string markdown, CancellationToken ct);

    Task UpdateMessageAsync(
        string botToken, string channel, string ts, string markdown, CancellationToken ct);

    Task<SlackUserInfo> UserInfoAsync(
        string botToken, string userId, CancellationToken ct);
}
