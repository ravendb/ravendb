using Raven.Quill.Channels;

namespace Raven.Quill.Contracts;

public sealed record ProvisionChannelRequest(
    ChannelType? Type,
    string AgentId,
    string[]? AllowedOrigins,
    string? DisplayName = null,
    TelegramProvisionRequest? Telegram = null,
    WhatsAppProvisionRequest? WhatsApp = null);

public sealed record TelegramProvisionRequest(
    string? BotToken,
    Dictionary<string, TelegramParameterBinding>? ParameterBindings = null);

public sealed record WhatsAppProvisionRequest(
    Dictionary<string, TelegramParameterBinding>? ParameterBindings = null);
