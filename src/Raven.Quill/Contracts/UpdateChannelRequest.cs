using Raven.Quill.Channels;

namespace Raven.Quill.Contracts;

public sealed record UpdateChannelRequest(
    string? DisplayName,
    string[]? AllowedOrigins,
    bool? Enabled,
    // Telegram only: rotates the bot token; never echoed back
    string? BotToken = null,
    // Telegram only: replaces the whole override set; null leaves it unchanged
    TelegramChannelMessages? Messages = null);
