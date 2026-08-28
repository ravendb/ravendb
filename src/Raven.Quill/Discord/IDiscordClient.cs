using System.Net;

namespace Raven.Quill.Discord;

internal sealed record DiscordBotIdentity(
    string ApplicationId,
    string BotUserId,
    string BotUsername);

internal sealed class DiscordApiException(
    string message, HttpStatusCode? status = null, TimeSpan? retryAfter = null, Exception? inner = null)
    : Exception(message, inner)
{
    public HttpStatusCode? Status { get; } = status;

    public TimeSpan? RetryAfter { get; } = retryAfter;

    internal bool RateLimited => Status == HttpStatusCode.TooManyRequests;
}

internal interface IDiscordClient
{
    Task<(DiscordBotIdentity? Identity, string? Error, bool DiscordResponded)> GetBotIdentityAsync(
        string botToken, CancellationToken ct);

    Task<string> GetGatewayUrlAsync(string botToken, CancellationToken ct);

    Task<string> CreateMessageAsync(string botToken, string channelId, string content, CancellationToken ct);

    Task EditMessageAsync(
        string botToken, string channelId, string messageId, string content, CancellationToken ct);
}
