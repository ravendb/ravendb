namespace Raven.Quill.Slack;

internal sealed record SlackAuthInfo(
    string TeamId,
    string TeamName,
    string BotUserId,
    string BotName);

internal sealed record SlackUserInfo(
    string UserId,
    string? Email);

internal sealed class SlackApiException(
    string message, string? error = null, TimeSpan? retryAfter = null, Exception? inner = null,
    bool slackResponded = false)
    : Exception(message, inner)
{
    internal const string RateLimitedError = "ratelimited";

    internal const string MissingScopeError = "missing_scope";

    public string? Error { get; } = error;

    public TimeSpan? RetryAfter { get; } = retryAfter;

    public bool SlackResponded { get; } = slackResponded;
}

internal interface ISlackClient
{
    Task<(SlackAuthInfo? Info, string? Error, bool SlackResponded)> AuthTestAsync(
        string botToken, CancellationToken ct);

    Task<string> PostMessageAsync(
        string botToken, string channel, string text, CancellationToken ct);

    Task UpdateMessageAsync(
        string botToken, string channel, string ts, string text, CancellationToken ct);

    Task<SlackUserInfo> UserInfoAsync(
        string botToken, string userId, CancellationToken ct);
}
