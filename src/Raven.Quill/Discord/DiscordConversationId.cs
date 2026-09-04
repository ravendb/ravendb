using Raven.Quill.Channels;

namespace Raven.Quill.Discord;

internal static class DiscordConversationId
{
    internal static string For(
        string channelId, string discordUserId, Dictionary<string, string> parameters) =>
        ChannelConversationId.For("discord", channelId, discordUserId, parameters);
}
