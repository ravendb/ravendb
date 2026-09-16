using System.Globalization;

namespace Raven.Quill.Slack;

internal static class SlackConversationId
{
    internal static string ForUtcDay(string channelId, string slackUserId, DateTime utcNow) =>
        string.Create(CultureInfo.InvariantCulture,
            $"chats/slack/{channelId}/{slackUserId}/{utcNow:yyyy-MM-dd}");
}
