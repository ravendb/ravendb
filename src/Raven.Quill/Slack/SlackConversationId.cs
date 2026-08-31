using Raven.Quill.Channels;

namespace Raven.Quill.Slack;

internal static class SlackConversationId
{
    internal static string ForUtcDay(
        string channelId, string slackUserId, DateTime utcNow, Dictionary<string, string> parameters) =>
        ChannelConversationId.ForUtcDay("slack", channelId, slackUserId, utcNow, parameters);
}
