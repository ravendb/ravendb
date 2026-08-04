using Raven.Quill.Channels;

namespace Raven.Quill.Contracts;

public sealed record ProvisionChannelRequest(
    ChannelType? Type,
    string AgentId,
    string[]? AllowedOrigins,
    string? DisplayName = null,
    string? BotToken = null,
    Dictionary<string, string>? Parameters = null);
