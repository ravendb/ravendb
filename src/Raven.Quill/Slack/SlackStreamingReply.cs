using Raven.Quill.Logging;
using Raven.Quill.Channels;
using Raven.Quill.Hosting;

namespace Raven.Quill.Slack;

internal sealed class SlackStreamingReply(
    ISlackClient slack,
    string botToken,
    string dmChannel,
    SlackOptions options,
    QuillLogger<SlackInboundProcessor> logger,
    CancellationToken ct) : ChannelStreamingReply(options.MessageLimit, options.EditDebounce)
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

    protected override async Task ShowPreviewAsync(string text)
    {
        if (_currentTs.Length == 0)
            _currentTs = await slack.PostMessageAsync(botToken, dmChannel, text, ct);
        else
            await slack.UpdateMessageAsync(botToken, dmChannel, _currentTs, text, ct);
    }

    protected override Task SendFinalAsync(string text) => PostWithRetryAsync(text);

    protected override Task EditFinalAsync(string text) =>
        text == LastShownText ? Task.CompletedTask : UpdateWithRetryAsync(_currentTs, text);

    private async Task PostWithRetryAsync(string text)
    {
        try
        {
            await slack.PostMessageAsync(botToken, dmChannel, text, ct);
        }
        catch (SlackApiException e) when (e.Error == SlackApiException.RateLimitedError)
        {
            await DelayForRetryAsync(e);
            await slack.PostMessageAsync(botToken, dmChannel, text, ct);
        }
    }

    private async Task UpdateWithRetryAsync(string ts, string text)
    {
        try
        {
            await slack.UpdateMessageAsync(botToken, dmChannel, ts, text, ct);
        }
        catch (SlackApiException e) when (e.Error == SlackApiException.RateLimitedError)
        {
            await DelayForRetryAsync(e);
            await slack.UpdateMessageAsync(botToken, dmChannel, ts, text, ct);
        }
    }

    private Task DelayForRetryAsync(SlackApiException e)
    {
        var delay = e.RetryAfter ?? TimeSpan.FromSeconds(1);
        return Task.Delay(delay > MaxRetryDelay ? MaxRetryDelay : delay, ct);
    }
}
