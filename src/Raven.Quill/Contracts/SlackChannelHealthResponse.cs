namespace Raven.Quill.Contracts;

public sealed record SlackChannelHealthResponse(
    string ChannelId,
    string TeamId,
    string TeamName,
    string BotUserId,
    bool Enabled,
    bool? TokenValid,
    string? TokenError,
    DateTime? LastInboundAt,
    DateTime? LastSignatureFailureAt,
    DateTime? LastSendErrorAt,
    string? LastSendError);
