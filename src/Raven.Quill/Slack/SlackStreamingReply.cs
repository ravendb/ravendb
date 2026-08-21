using Raven.Quill.Channels;
using Raven.Quill.Hosting;

namespace Raven.Quill.Slack;

internal sealed class SlackStreamingReply(
    ISlackClient slack,
    string botToken,
    string dmChannel,
    SlackOptions options,
    ILogger logger,
    CancellationToken ct) : ChannelStreamingReply(options.MessageLimit, options.EditDebounce)
{
    private static readonly TimeSpan MaxRetryDelay = TimeSpan.FromSeconds(60);

    private string _currentTs = "";

    protected override bool HasOpenMessage => _currentTs.Length > 0;

    protected override void CloseCurrentMessage() => _currentTs = "";

    protected override void LogFlushFailure(Exception error) =>
        logger.LogDebug("Slack streaming flush failed for channel {DmChannel}: {Error}", dmChannel, error.Message);

    protected override async Task ShowPreviewAsync(string text)
    {
        var escaped = SlackMrkdwn.Escape(text);
        if (_currentTs.Length == 0)
            _currentTs = await slack.PostMessageAsync(botToken, dmChannel, escaped, ct);
        else
            await slack.UpdateMessageAsync(botToken, dmChannel, _currentTs, escaped, ct);
    }

    protected override async Task SendFinalAsync(string text)
    {
        var converted = SlackMrkdwn.Convert(text);
        try
        {
            await PostWithRetryAsync(converted);
        }
        catch (SlackApiException e) when (e.Error != SlackApiException.RateLimitedError && converted != text)
        {
            await PostWithRetryAsync(SlackMrkdwn.Escape(text));
        }
    }

    protected override async Task EditFinalAsync(string text)
    {
        var converted = SlackMrkdwn.Convert(text);
        if (converted == LastShownText)
            return;

        try
        {
            await UpdateWithRetryAsync(_currentTs, converted);
        }
        catch (SlackApiException e) when (e.Error != SlackApiException.RateLimitedError && converted != text)
        {
            if (text == LastShownText)
                return;
            await UpdateWithRetryAsync(_currentTs, SlackMrkdwn.Escape(text));
        }
    }

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
