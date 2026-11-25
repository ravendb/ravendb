using Raven.Quill.Logging;
using SlackNet;

namespace Raven.Quill.Slack;

internal sealed class SlackNetLogger(SlackRuntime runtime, string shortChannelId, QuillLogger<SlackRuntime> logger)
    : SlackNet.ILogger
{
    private const string ReconnectingWebSocketSource = "SlackNet.ReconnectingWebSocket";

    public void Log(ILogEvent logEvent)
    {
        if (IsReconnectRetry(logEvent))
        {
            runtime.OnReconnectRetryFailed(logEvent.Exception!);
            return;
        }

        if (logEvent.Category != LogCategory.Error)
            return;

        if (IsReconnectGiveUp(logEvent))
            runtime.OnReconnectGiveUp(logEvent.Exception!);

        if (logger.IsWarnEnabled == false)
            return;

        var message = string.Format(logEvent.IndexedMessageTemplate(), logEvent.MessagePropertyValues());
        logger.Warn(logEvent.Exception is { } e
            ? $"SlackNet error for channel {shortChannelId}: {message}: {e.Message}"
            : $"SlackNet error for channel {shortChannelId}: {message}");
    }

    private static bool IsReconnectRetry(ILogEvent logEvent) =>
        logEvent.Exception is not null &&
        IsFromReconnectingWebSocket(logEvent) &&
        logEvent.MessageTemplate.Contains("retrying in", StringComparison.Ordinal);

    private static bool IsReconnectGiveUp(ILogEvent logEvent) =>
        logEvent.Exception is SlackException or TaskCanceledException &&
        IsFromReconnectingWebSocket(logEvent);

    private static bool IsFromReconnectingWebSocket(ILogEvent logEvent) =>
        logEvent.Properties.TryGetValue("Source", out var source) &&
        source is ReconnectingWebSocketSource;
}
