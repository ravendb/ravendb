using System.Net;
using System.Text.Json;
using Raven.Client.Exceptions;

namespace Raven.Quill.Agents;

public enum ProviderFailureKind
{
    RateLimited,
    QuotaExhausted,
    Credentials,
    Unavailable,
    Protocol,
    Refused,
    Timeout,
    Unknown,
}

public sealed record ProviderFailure(
    ProviderFailureKind Kind,
    bool Retryable,
    string VisitorMessage,
    string OperatorMessage,
    TimeSpan? RetryAfter = null)
{
    public string Code => Retryable ? RetryableCode : FailedCode;

    public const string RetryableCode = "provider_busy";
    public const string FailedCode = "chat_failed";
}

public sealed class ProviderTimeoutException(TimeSpan limit)
    : Exception($"the AI provider did not answer within {limit.TotalSeconds:0.#}s");

public sealed class EmptyAnswerException() : Exception("the AI provider returned an answer with no content");

public static class ProviderFailures
{
    public static ProviderFailure Classify(Exception exception)
    {
        for (var e = exception; e is not null; e = e.InnerException)
        {
            if (Match(e) is { } failure)
                return failure;
        }

        return Unknown;
    }

    private static ProviderFailure? Match(Exception e) => e switch
    {
        RateLimitException rate => RateLimited(rate.RetryAfter > TimeSpan.Zero ? rate.RetryAfter : null),
        InsufficientQuotaException => QuotaExhausted,
        TooManyRequestsException => RateLimited(null),
        RefusedToAnswerException => Refused,
        UnsuccessfulAiRequestException unsuccessful => FromStatus(unsuccessful.StatusCode),
        ProviderTimeoutException => TimedOut,
        EmptyAnswerException => Protocol,
        InvalidDataException or JsonException => Protocol,
        HttpRequestException or IOException => Unavailable,
        _ => null,
    };

    private static ProviderFailure FromStatus(HttpStatusCode status) => (int)status switch
    {
        401 or 403 or 407 => Credentials,
        402 => QuotaExhausted,
        429 => RateLimited(null),
        408 or 502 or 503 or 504 => Unavailable,
        >= 500 => Unavailable,
        400 or 404 or 422 => Protocol,
        _ => Unknown,
    };

    private static ProviderFailure RateLimited(TimeSpan? retryAfter) => new(
        ProviderFailureKind.RateLimited,
        Retryable: true,
        "The assistant is busy right now. Please try again in a moment.",
        "the AI provider rate-limited the request",
        retryAfter);

    private static readonly ProviderFailure QuotaExhausted = new(
        ProviderFailureKind.QuotaExhausted,
        Retryable: false,
        "The assistant is unavailable right now. Please try again later.",
        "the AI provider reports this account is out of quota; check the provider's billing");

    private static readonly ProviderFailure Credentials = new(
        ProviderFailureKind.Credentials,
        Retryable: false,
        "The assistant is unavailable right now. Please try again later.",
        "the AI provider rejected the credentials; check the API key on the agent's connection string");

    private static readonly ProviderFailure Unavailable = new(
        ProviderFailureKind.Unavailable,
        Retryable: true,
        "The assistant is temporarily unavailable. Please try again in a moment.",
        "the AI provider is unreachable or returning errors");

    private static readonly ProviderFailure Protocol = new(
        ProviderFailureKind.Protocol,
        Retryable: false,
        "The assistant could not complete that answer. Please try again.",
        "the AI provider's response could not be read");

    private static readonly ProviderFailure Refused = new(
        ProviderFailureKind.Refused,
        Retryable: false,
        "The assistant could not answer that.",
        "the model refused to answer the prompt");

    private static readonly ProviderFailure TimedOut = new(
        ProviderFailureKind.Timeout,
        Retryable: true,
        "The assistant took too long to answer. Please try again.",
        "the AI provider did not answer within the time limit");

    private static readonly ProviderFailure Unknown = new(
        ProviderFailureKind.Unknown,
        Retryable: false,
        "Something went wrong. Please try again.",
        "the chat turn failed");
}
