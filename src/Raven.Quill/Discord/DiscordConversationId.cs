using Raven.Quill.Channels;

namespace Raven.Quill.Discord;

internal static class DiscordConversationId
{
    internal static string ForUtcDay(
        string channelId, string discordUserId, DateTime utcNow, Dictionary<string, string> parameters) =>
        ChannelConversationId.ForUtcDay("discord", channelId, discordUserId, utcNow, parameters);
}
