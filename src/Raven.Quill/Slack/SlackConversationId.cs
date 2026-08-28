using System.Globalization;
using Raven.Quill.Channels;

namespace Raven.Quill.Slack;

internal static class SlackConversationId
{
    internal static string ForUtcDay(
        string channelId, string slackUserId, DateTime utcNow,
        Dictionary<string, ChannelParameterBinding> parameterBindings) =>
        string.Create(CultureInfo.InvariantCulture,
            $"chats/slack/{channelId}/{slackUserId}/{utcNow:yyyy-MM-dd}/{ChannelParameterBindings.Fingerprint(parameterBindings)}");
}
