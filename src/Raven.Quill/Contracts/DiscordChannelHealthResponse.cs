namespace Raven.Quill.Contracts;

public sealed record DiscordChannelHealthResponse(
    string ChannelId,
    string ApplicationId,
    string BotUserId,
    string BotUsername,
    bool Enabled,
    bool? TokenValid,
    string? TokenError,
    bool GatewayConnected,
    DateTime? LastConnectedAt,
    string? LastGatewayError,
    DateTime? LastInboundAt,
    DateTime? LastSendErrorAt,
    string? LastSendError);
