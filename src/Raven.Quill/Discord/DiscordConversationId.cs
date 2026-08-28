using System.Globalization;
using Raven.Quill.Channels;

namespace Raven.Quill.Discord;

internal static class DiscordConversationId
{
    internal static string ForUtcDay(
        string channelId, string discordUserId, DateTime utcNow,
        Dictionary<string, ChannelParameterBinding> parameterBindings) =>
        string.Create(CultureInfo.InvariantCulture,
            $"chats/discord/{channelId}/{discordUserId}/{utcNow:yyyy-MM-dd}/{ChannelParameterBindings.Fingerprint(parameterBindings)}");
}
