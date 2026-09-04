using Raven.Quill.Channels;

namespace Raven.Quill.Slack;

internal static class SlackConversationId
{
    internal static string For(
        string channelId, string slackUserId, Dictionary<string, string> parameters) =>
        ChannelConversationId.For("slack", channelId, slackUserId, parameters);
}
