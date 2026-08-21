namespace Raven.Quill.Slack;

internal sealed record SlackAuthInfo(
    string TeamId,
    string TeamName,
    string BotUserId,
    string BotName);

internal sealed class SlackApiException(
    string message, string? error = null, TimeSpan? retryAfter = null, Exception? inner = null)
    : Exception(message, inner)
{
    internal const string RateLimitedError = "ratelimited";

    public string? Error { get; } = error;

    public TimeSpan? RetryAfter { get; } = retryAfter;
}

internal interface ISlackClient
{
    Task<(SlackAuthInfo? Info, string? Error, bool SlackResponded)> AuthTestAsync(
        string botToken, CancellationToken ct);

    Task<string> PostMessageAsync(
        string botToken, string channel, string text, CancellationToken ct);

    Task UpdateMessageAsync(
        string botToken, string channel, string ts, string text, CancellationToken ct);
}
