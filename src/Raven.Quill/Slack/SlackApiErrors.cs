using Newtonsoft.Json;
using SlackNet;

namespace Raven.Quill.Slack;

internal static class SlackApiErrors
{
    internal const string MissingScope = "missing_scope";

    private const string RateLimited = "ratelimited";

    public static bool IsApiFailure(Exception e) =>
        e is SlackException or SlackRateLimitException or HttpRequestException or JsonException || IsTimeout(e);

    public static bool IsRateLimited(Exception e) => e is SlackRateLimitException or SlackException { ErrorCode: RateLimited };

    public static bool IsRefusal(Exception e) => e is SlackException && IsRateLimited(e) == false;

    public static string? ErrorCodeOf(Exception e) => (e as SlackException)?.ErrorCode;

    public static TimeSpan? RetryAfterOf(Exception e) => (e as SlackRateLimitException)?.RetryAfter;

    public static string Describe(Exception e) => e switch
    {
        _ when IsRateLimited(e) => "slack is rate-limiting requests; try again shortly",
        SlackException refused => $"slack refused the request: {refused.ErrorCode}",
        HttpRequestException { StatusCode: { } status } when (int)status >= 500 =>
            $"the Slack API is unavailable (status {(int)status})",
        HttpRequestException unreachable => $"could not reach the Slack API: {unreachable.Message}",
        _ when IsTimeout(e) => "the Slack API did not respond in time",
        JsonException => "slack returned a response that could not be read",
        _ => e.Message,
    };

    public static string DescribeBotTokenError(Exception e) => e switch
    {
        _ when IsRateLimited(e) => "slack is rate-limiting the token check; try again shortly",
        SlackException { ErrorCode: "invalid_auth" or "token_revoked" or "account_inactive" } =>
            "slack rejected the bot token; copy the xoxb- token from the app's OAuth page and try again",
        SlackException refused => $"slack refused the token check: {refused.ErrorCode}",
        _ => Describe(e),
    };

    public static string DescribeAppTokenCheckError(Exception e) => e switch
    {
        _ when IsRateLimited(e) => "slack is rate-limiting the app-level token check; try again shortly",
        SlackException refused => DescribeAppTokenError(refused.ErrorCode) ??
                                  $"slack refused the app-level token check: {refused.ErrorCode}",
        _ => Describe(e),
    };

    public static string? DescribeAppTokenError(string? code) => code switch
    {
        "not_allowed_token_type" =>
            "slack refused the app-level token because it is the wrong token type; paste the xapp- token from the app's Basic Information page",
        MissingScope =>
            "the app-level token lacks the connections:write scope; regenerate it with that scope on the app's Basic Information page",
        "invalid_auth" or "not_authed" or "token_revoked" or "account_inactive" or "token_expired" =>
            "slack rejected the app-level token; regenerate it on the app's Basic Information page and rotate it on this channel",
        _ => null,
    };

    public static bool AppTokenErrorIsFixableInSlack(string? code) => code is MissingScope or "not_allowed_token_type";

    // HttpClient reports its own timeout as a TaskCanceledException wrapping a TimeoutException
    private static bool IsTimeout(Exception e) => e is TaskCanceledException { InnerException: TimeoutException };
}
