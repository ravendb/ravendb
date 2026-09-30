using System.Globalization;

namespace Raven.Quill.Discord;

internal static class DiscordConversationId
{
    internal static string ForUtcDay(string channelId, string discordUserId, DateTime utcNow) =>
        string.Create(CultureInfo.InvariantCulture,
            $"chats/discord/{channelId}/{discordUserId}/{utcNow:yyyy-MM-dd}");
}
