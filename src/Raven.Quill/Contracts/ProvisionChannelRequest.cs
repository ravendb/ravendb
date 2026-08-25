using Raven.Quill.Channels;

namespace Raven.Quill.Contracts;

public sealed record ProvisionChannelRequest(
    ChannelType? Type,
    string AgentId,
    string[]? AllowedOrigins,
    string? DisplayName = null,
    TelegramProvisionRequest? Telegram = null,
    SlackProvisionRequest? Slack = null);

public sealed record TelegramProvisionRequest(
    string? BotToken,
    Dictionary<string, ChannelParameterBinding>? ParameterBindings = null);

public sealed record SlackProvisionRequest(
    string? BotToken,
    string? SigningSecret,
    Dictionary<string, ChannelParameterBinding>? ParameterBindings = null);
