using Newtonsoft.Json;
using SlackNet;

namespace Raven.Quill.Slack;

internal static class SlackApiErrors
{
    public static SlackApiException? Translate(Exception e, string method) => e switch
    {
        SlackRateLimitException rateLimited => new SlackApiException(
            $"slack rate-limited {method}", SlackApiException.RateLimitedError, rateLimited.RetryAfter, rateLimited,
            slackResponded: true),
        SlackException refused => new SlackApiException(
            $"slack refused {method}: {refused.ErrorCode}", refused.ErrorCode, inner: refused, slackResponded: true),
        HttpRequestException { StatusCode: { } status } unavailable when (int)status >= 500 => new SlackApiException(
            $"the Slack API is unavailable (status {(int)status})", inner: unavailable),
        HttpRequestException unreachable => new SlackApiException(
            $"could not reach the Slack API: {unreachable.Message}", inner: unreachable),
        JsonException unparseable => new SlackApiException(
            $"slack returned an unrecognized {method} payload", inner: unparseable, slackResponded: true),
        NullReferenceException malformed => new SlackApiException(
            $"slack returned an unrecognized {method} response", inner: malformed, slackResponded: true),
        OperationCanceledException => new SlackApiException($"the Slack API did not respond to {method}"),
        _ => null,
    };

    public static string? DescribeAppTokenError(string? code) => code switch
    {
        "not_allowed_token_type" =>
            "slack refused the app-level token because it is the wrong token type; paste the xapp- token from the app's Basic Information page",
        "missing_scope" =>
            "the app-level token lacks the connections:write scope; regenerate it with that scope on the app's Basic Information page",
        "invalid_auth" or "not_authed" or "token_revoked" or "account_inactive" or "token_expired" =>
            "slack rejected the app-level token; regenerate it on the app's Basic Information page and rotate it on this channel",
        _ => null,
    };

    public static bool AppTokenErrorIsFixableInSlack(string? code) => code is "missing_scope" or "not_allowed_token_type";
}
