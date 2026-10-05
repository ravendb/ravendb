using Raven.Quill.Logging;
using Raven.Quill.Channels;
using Raven.Quill.Hosting;

namespace Raven.Quill.Slack;

internal sealed class SlackStreamingReply(
    ISlackClient slack,
    string botToken,
    string dmChannel,
    SlackOptions options,
    QuillLogger<SlackInboundProcessor> logger) : ChannelStreamingReply(options.MessageLimit, options.EditDebounce)
{
    private static readonly TimeSpan MaxRetryDelay = TimeSpan.FromSeconds(60);

    private string _currentTs = "";

    protected override bool HasOpenMessage => _currentTs.Length > 0;

    protected override void CloseCurrentMessage() => _currentTs = "";

    protected override void LogFlushFailure(Exception error)
    {
        if (logger.IsDebugEnabled)
            logger.Debug($"Slack streaming flush failed for channel {dmChannel}: {error.Message}");
    }

    protected override async Task ShowPreviewAsync(string text, CancellationToken token)
    {
        if (_currentTs.Length == 0)
            _currentTs = await slack.PostMessageAsync(botToken, dmChannel, text, token);
        else
            await slack.UpdateMessageAsync(botToken, dmChannel, _currentTs, text, token);
    }

    protected override Task SendFinalAsync(string text, CancellationToken token) => PostWithRetryAsync(text, token);

    protected override Task EditFinalAsync(string text, CancellationToken token) =>
        text == LastShownText ? Task.CompletedTask : UpdateWithRetryAsync(_currentTs, text, token);

    private async Task PostWithRetryAsync(string text, CancellationToken token)
    {
        try
        {
            await slack.PostMessageAsync(botToken, dmChannel, text, token);
        }
        catch (Exception e) when (SlackApiErrors.IsRateLimited(e))
        {
            await DelayForRetryAsync(e, token);
            await slack.PostMessageAsync(botToken, dmChannel, text, token);
        }
    }

    private async Task UpdateWithRetryAsync(string ts, string text, CancellationToken token)
    {
        try
        {
            await slack.UpdateMessageAsync(botToken, dmChannel, ts, text, token);
        }
        catch (Exception e) when (SlackApiErrors.IsRateLimited(e))
        {
            await DelayForRetryAsync(e, token);
            await slack.UpdateMessageAsync(botToken, dmChannel, ts, text, token);
        }
    }

    private static Task DelayForRetryAsync(Exception e, CancellationToken token)
    {
        var delay = SlackApiErrors.RetryAfterOf(e) ?? TimeSpan.FromSeconds(1);
        return Task.Delay(delay > MaxRetryDelay ? MaxRetryDelay : delay, token);
    }
}
