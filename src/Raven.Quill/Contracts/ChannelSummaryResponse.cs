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
    TelegramSummaryResponse? Telegram = null)
{
    internal static ChannelSummaryResponse From(Channel channel) => new(
        channel.ShortId,
        channel.Type,
        channel.AgentId,
        channel.DisplayName,
        channel.Enabled,
        channel.CreatedAt,
        channel.Telegram is null
            ? null
            : new TelegramSummaryResponse(
                channel.Telegram.BotUsername,
                channel.Telegram.ParameterBindings,
                channel.Telegram.Messages));
}

public sealed record TelegramSummaryResponse(
    string BotUsername,
    Dictionary<string, TelegramParameterBinding> ParameterBindings,
    TelegramChannelMessages? Messages);
