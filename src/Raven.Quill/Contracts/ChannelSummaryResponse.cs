using Raven.Quill.Channels;

namespace Raven.Quill.Contracts;

// no secrets: never projects binding id / allowed-origins / bot token
public sealed record ChannelSummaryResponse(
    string ChannelId,
    ChannelType Type,
    string AgentId,
    string DisplayName,
    bool Enabled,
    DateTime CreatedAt,
    string? BotUsername = null,
    Dictionary<string, TelegramParameterBinding>? ParameterBindings = null,
    TelegramChannelMessages? Messages = null)
{
    internal static ChannelSummaryResponse From(Channel channel) => new(
        Channel.StripIdPrefix(channel.Id),
        channel.Type,
        channel.AgentId,
        channel.DisplayName,
        channel.Enabled,
        channel.CreatedAt,
        channel.Telegram?.BotUsername,
        channel.Telegram?.ParameterBindings,
        channel.Telegram?.Messages);
}
