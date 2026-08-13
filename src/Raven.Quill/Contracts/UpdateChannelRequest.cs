using Raven.Quill.Channels;

namespace Raven.Quill.Contracts;

public sealed record UpdateChannelRequest(
    string? DisplayName,
    string[]? AllowedOrigins,
    bool? Enabled,
    TelegramUpdateRequest? Telegram = null);

public sealed record TelegramUpdateRequest(
    string? BotToken = null,
    TelegramChannelMessages? Messages = null,
    Dictionary<string, TelegramParameterBinding>? ParameterBindings = null);
