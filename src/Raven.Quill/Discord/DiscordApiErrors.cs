using System.Net;
using System.Text.Json;
using Discord.Net;

namespace Raven.Quill.Discord;

internal sealed class DiscordApiException(string message, HttpStatusCode? status = null, Exception? inner = null)
    : Exception(message, inner)
{
    public HttpStatusCode? Status { get; } = status;
}

internal static class DiscordApiErrors
{
    internal const int InvalidTokenClose = 4004;

    internal const string TokenRejected =
        "discord rejected the bot token; reset it on the app's Bot page and try again";

    public static bool IsApiFailure(Exception e) =>
        e is HttpException or RateLimitedException or TimeoutException or OperationCanceledException or HttpRequestException
            or JsonException;

    public static bool IsTokenRejected(Exception e) => e switch
    {
        HttpException { HttpCode: HttpStatusCode.Unauthorized } => true,
        DiscordApiException { Status: HttpStatusCode.Unauthorized } => true,
        _ => CloseOf(e)?.CloseCode == InvalidTokenClose,
    };

    public static bool IsFixableInDiscord(int closeCode) => closeCode is 4013 or 4014;

    public static bool Responded(Exception e) => e is HttpException refused && (int)refused.HttpCode < 500;

    public static HttpStatusCode? StatusOf(Exception e) => e switch
    {
        RateLimitedException => HttpStatusCode.TooManyRequests,
        HttpException http => http.HttpCode,
        _ => null,
    };

    public static string Describe(Exception e, string what) => e switch
    {
        _ when IsTokenRejected(e) => TokenRejected,
        RateLimitedException => $"discord is rate-limiting {what}; try again shortly",
        HttpException http when (int)http.HttpCode >= 500 => $"the Discord API is unavailable (status {(int)http.HttpCode})",
        HttpException refused => $"discord refused {what}: {ErrorTextOf(refused)}",
        TimeoutException or OperationCanceledException => $"the Discord API did not respond to {what}",
        HttpRequestException unreachable => $"could not reach the Discord API: {unreachable.Message}",
        JsonException => "discord returned a response that could not be read",
        _ => e.Message,
    };

    public static WebSocketClosedException? CloseOf(Exception e) =>
        e as WebSocketClosedException ?? e.InnerException as WebSocketClosedException;

    private static string ErrorTextOf(HttpException e) =>
        string.IsNullOrWhiteSpace(e.Reason) ? $"status {(int)e.HttpCode}" : e.Reason;
}
