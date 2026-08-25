using Raven.Quill.Channels;

namespace Raven.Quill.Contracts;

public sealed record UpdateChannelRequest(
    string? DisplayName,
    string[]? AllowedOrigins,
    bool? Enabled,
    TelegramUpdateRequest? Telegram = null,
    SlackUpdateRequest? Slack = null);

public sealed record TelegramUpdateRequest(
    string? BotToken = null,
    TelegramChannelMessages? Messages = null,
    Dictionary<string, ChannelParameterBinding>? ParameterBindings = null);

public sealed record SlackUpdateRequest(
    string? BotToken = null,
    string? SigningSecret = null,
    Dictionary<string, ChannelParameterBinding>? ParameterBindings = null);
