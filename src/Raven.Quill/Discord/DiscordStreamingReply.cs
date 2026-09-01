using Raven.Quill.Logging;
using Raven.Quill.Channels;
using Raven.Quill.Hosting;

namespace Raven.Quill.Discord;

internal sealed class DiscordStreamingReply(
    IDiscordClient discord,
    string botToken,
    string dmChannelId,
    DiscordOptions options,
    QuillLogger<DiscordInboundProcessor> logger,
    CancellationToken ct) : ChannelStreamingReply(options.MessageLimit, options.EditDebounce)
{
    private static readonly TimeSpan MaxRetryDelay = TimeSpan.FromSeconds(60);

    private string _currentMessageId = "";

    protected override bool HasOpenMessage => _currentMessageId.Length > 0;

    protected override void CloseCurrentMessage() => _currentMessageId = "";

    protected override void LogFlushFailure(Exception error)
    {
        if (logger.IsDebugEnabled)
            logger.Debug($"Discord streaming flush failed for channel {dmChannelId}: {error.Message}");
    }

    protected override async Task ShowPreviewAsync(string text)
    {
        if (_currentMessageId.Length == 0)
            _currentMessageId = await discord.CreateMessageAsync(botToken, dmChannelId, text, ct);
        else
            await discord.EditMessageAsync(botToken, dmChannelId, _currentMessageId, text, ct);
    }

    protected override Task SendFinalAsync(string text) => CreateWithRetryAsync(text);

    protected override Task EditFinalAsync(string text) =>
        text == LastShownText ? Task.CompletedTask : EditWithRetryAsync(_currentMessageId, text);

    private async Task CreateWithRetryAsync(string text)
    {
        try
        {
            await discord.CreateMessageAsync(botToken, dmChannelId, text, ct);
        }
        catch (DiscordApiException e) when (e.RateLimited)
        {
            await DelayForRetryAsync(e);
            await discord.CreateMessageAsync(botToken, dmChannelId, text, ct);
        }
    }

    private async Task EditWithRetryAsync(string messageId, string text)
    {
        try
        {
            await discord.EditMessageAsync(botToken, dmChannelId, messageId, text, ct);
        }
        catch (DiscordApiException e) when (e.RateLimited)
        {
            await DelayForRetryAsync(e);
            await discord.EditMessageAsync(botToken, dmChannelId, messageId, text, ct);
        }
    }

    private Task DelayForRetryAsync(DiscordApiException e)
    {
        var delay = e.RetryAfter ?? TimeSpan.FromSeconds(1);
        return Task.Delay(delay > MaxRetryDelay ? MaxRetryDelay : delay, ct);
    }
}
