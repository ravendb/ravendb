using Raven.Quill.Logging;
using SlackNet;

namespace Raven.Quill.Slack;

internal sealed class SlackNetLogger(string shortChannelId, QuillLogger<SlackChannelManager> logger) : SlackNet.ILogger
{
    public void Log(ILogEvent logEvent)
    {
        if (logEvent.Category != LogCategory.Error || logger.IsWarnEnabled == false)
            return;

        var message = string.Format(logEvent.IndexedMessageTemplate(), logEvent.MessagePropertyValues());
        logger.Warn(logEvent.Exception is { } e
            ? $"SlackNet error for channel {shortChannelId}: {message}: {e.Message}"
            : $"SlackNet error for channel {shortChannelId}: {message}");
    }
}
