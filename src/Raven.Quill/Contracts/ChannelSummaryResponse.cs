using Raven.Quill.Channels;

namespace Raven.Quill.Contracts;

// no secrets: never projects binding id / bot token / Slack credentials
public sealed record ChannelSummaryResponse(
    string ChannelId,
    ChannelType Type,
    string AgentId,
    string DisplayName,
    bool Enabled,
    DateTime CreatedAt,
    string[] AllowedOrigins,
    TelegramSummaryResponse? Telegram = null,
    SlackSummaryResponse? Slack = null)
{
    internal static ChannelSummaryResponse From(Channel channel) => new(
        channel.ShortId,
        channel.Type,
        channel.AgentId,
        channel.DisplayName,
        channel.Enabled,
        channel.CreatedAt,
        channel.AllowedOrigins,
        channel.Telegram is null
            ? null
            : new TelegramSummaryResponse(
                channel.Telegram.BotUsername,
                channel.Telegram.ParameterBindings,
                channel.Telegram.Messages),
        channel.Slack is null
            ? null
            : new SlackSummaryResponse(
                channel.Slack.TeamId,
                channel.Slack.TeamName,
                channel.Slack.BotUserId,
                channel.Slack.ParameterBindings));
}

public sealed record TelegramSummaryResponse(
    string BotUsername,
    Dictionary<string, ChannelParameterBinding> ParameterBindings,
    TelegramChannelMessages? Messages);

public sealed record SlackSummaryResponse(
    string TeamId,
    string TeamName,
    string BotUserId,
    Dictionary<string, ChannelParameterBinding> ParameterBindings);
