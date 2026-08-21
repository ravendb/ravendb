using System.Text;
using Raven.Quill.Hosting;
using Raven.Quill.Telegram;

namespace Raven.Quill.Slack;

internal sealed class SlackStreamingReply(
    ISlackClient slack,
    string botToken,
    string dmChannel,
    SlackOptions options,
    ILogger logger,
    CancellationToken ct)
{
    private static readonly TimeSpan MaxRetryDelay = TimeSpan.FromSeconds(10);

    private readonly StringBuilder _buffer = new();
    private string _currentTs = "";
    private int _flushedUpTo;
    private string _lastShownText = "";
    private DateTime _lastFlushAt;

    private int PendingLength => _buffer.Length - _flushedUpTo;

    public bool IsEmpty => _buffer.Length == 0;

    public async ValueTask OnChunkAsync(string chunk)
    {
        _buffer.Append(chunk);

        if (PendingLength <= options.MessageLimit &&
            DateTime.UtcNow - _lastFlushAt < options.EditDebounce)
            return;

        try
        {
            await FlushPreviewAsync();
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            logger.LogDebug("Slack streaming flush failed for channel {DmChannel}: {Error}", dmChannel, e.Message);
        }
        finally
        {
            _lastFlushAt = DateTime.UtcNow;
        }
    }

    private async Task FlushPreviewAsync()
    {
        while (PendingLength > options.MessageLimit)
        {
            var pending = _buffer.ToString(_flushedUpTo, PendingLength);
            var cut = TelegramMessageSplitter.CutPoint(pending, options.MessageLimit);

            var segment = pending[..cut].TrimEnd();
            if (_currentTs.Length == 0)
                await SendSafeAsync(segment);
            else
                await EditSafeAsync(_currentTs, segment);
            _flushedUpTo += cut;
            while (PendingLength > 0 && char.IsWhiteSpace(_buffer[_flushedUpTo]))
                _flushedUpTo++;

            _currentTs = "";
            _lastShownText = "";
        }

        await ShowPreviewAsync(_buffer.ToString(_flushedUpTo, PendingLength));
    }

    private async Task ShowPreviewAsync(string text)
    {
        if (text.Length == 0 || text == _lastShownText)
            return;

        var escaped = SlackMrkdwn.Escape(text);
        if (_currentTs.Length == 0)
            _currentTs = await slack.PostMessageAsync(botToken, dmChannel, escaped, ct);
        else
            await slack.UpdateMessageAsync(botToken, dmChannel, _currentTs, escaped, ct);

        _lastShownText = text;
    }

    public async Task FinalizeAsync()
    {
        var pending = _buffer.ToString(_flushedUpTo, PendingLength);
        if (string.IsNullOrWhiteSpace(pending))
            return;

        var parts = TelegramMessageSplitter.Split(pending, options.MessageLimit);
        for (var i = 0; i < parts.Count; i++)
        {
            if (i == 0 && _currentTs.Length > 0)
                await EditSafeAsync(_currentTs, parts[i]);
            else
                await SendSafeAsync(parts[i]);
        }
    }

    private async Task SendSafeAsync(string text)
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

    private async Task EditSafeAsync(string ts, string text)
    {
        var converted = SlackMrkdwn.Convert(text);
        if (converted == _lastShownText)
            return;

        try
        {
            await UpdateWithRetryAsync(ts, converted);
        }
        catch (SlackApiException e) when (e.Error != SlackApiException.RateLimitedError && converted != text)
        {
            if (text == _lastShownText)
                return;
            await UpdateWithRetryAsync(ts, SlackMrkdwn.Escape(text));
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
