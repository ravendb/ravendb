using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using System.Text.RegularExpressions;
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
    string OperatorMessage)
{
    public string Code => Retryable ? RetryableCode : FailedCode;

    public const string RetryableCode = "provider_busy";
    public const string FailedCode = "chat_failed";
}

public sealed class ProviderTimeoutException(TimeSpan limit)
    : Exception($"the AI provider did not answer within {limit.TotalSeconds:0.#}s");

public sealed class EmptyAnswerException() : Exception("the AI provider returned an answer with no content");

public static partial class ProviderFailures
{
    public static ProviderFailure Classify(Exception exception)
    {
        var text = exception.ToString();

        if (Names(text, nameof(RateLimitException), nameof(TooManyRequestsException), nameof(TooManyTokensException)))
            return RateLimited;
        if (Names(text, nameof(InsufficientQuotaException)))
            return QuotaExhausted;
        if (Names(text, nameof(RefusedToAnswerException)))
            return Refused;
        if (Names(text, nameof(UnsuccessfulAiRequestException)))
            return StatusIn(text) is { } status ? FromStatus(status) : Unknown;
        if (Names(text, nameof(ProviderTimeoutException), nameof(TaskCanceledException), nameof(TimeoutException)))
            return TimedOut;
        if (Names(text, nameof(EmptyAnswerException), nameof(InvalidDataException), nameof(JsonException), "UnexpectedResponseException"))
            return Protocol;
        if (Names(text, nameof(HttpRequestException), nameof(SocketException), nameof(IOException)))
            return Unavailable;

        return Unknown;
    }

    private static bool Names(string text, params string[] typeNames) =>
        typeNames.Any(name => text.Contains($".{name}:", StringComparison.Ordinal));

    private static HttpStatusCode? StatusIn(string text)
    {
        var match = StatusCode().Match(text);
        return match.Success && Enum.TryParse<HttpStatusCode>(match.Groups[1].Value, out var status) ? status : null;
    }

    [GeneratedRegex(@"Status Code: (\w+)")]
    private static partial Regex StatusCode();

    private static ProviderFailure FromStatus(HttpStatusCode status) => (int)status switch
    {
        401 or 403 or 407 => Credentials,
        402 => QuotaExhausted,
        429 => RateLimited,
        408 or 502 or 503 or 504 => Unavailable,
        >= 500 => Unavailable,
        400 or 404 or 422 => Protocol,
        _ => Unknown,
    };

    private static readonly ProviderFailure RateLimited = new(
        ProviderFailureKind.RateLimited,
        Retryable: true,
        "The assistant is busy right now. Please try again in a moment.",
        "the AI provider rate-limited the request");

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
