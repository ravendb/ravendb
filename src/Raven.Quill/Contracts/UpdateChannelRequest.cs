namespace Raven.Quill.Contracts;

public sealed record UpdateChannelRequest(
    string? DisplayName,
    string[]? AllowedOrigins,
    bool? Enabled,
    // Telegram only: rotates the bot token; never echoed back
    string? BotToken = null);
