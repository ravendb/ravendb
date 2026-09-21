namespace Raven.Quill.Contracts;

public sealed record SlackChannelHealthResponse(
    string ChannelId,
    string TeamId,
    string TeamName,
    string BotUserId,
    bool Enabled,
    bool? TokenValid,
    string? TokenError,
    bool SocketConnected,
    DateTime? LastConnectedAt,
    string? LastSocketError,
    DateTime? LastInboundAt,
    DateTime? LastSendErrorAt,
    string? LastSendError);
