using Raven.Quill.Logging;
using Raven.Quill.Channels;
using Raven.Quill.Hosting;

namespace Raven.Quill.Discord;

internal sealed class DiscordStreamingReply(
    IDiscordClient discord,
    string botToken,
    string dmChannelId,
    DiscordOptions options,
    QuillLogger<DiscordTurns> logger) : ChannelStreamingReply(options.MessageLimit, options.EditDebounce)
{
    private static readonly TimeSpan MaxRetryDelay = TimeSpan.FromSeconds(60);

    private string _currentMessageId = "";
    private DateTime _previewCooldownUntil;

    protected override bool HasOpenMessage => _currentMessageId.Length > 0;

    protected override bool CanPreview => DateTime.UtcNow >= _previewCooldownUntil;

    protected override void CloseCurrentMessage() => _currentMessageId = "";

    protected override void LogFlushFailure(Exception error)
    {
        if (logger.IsDebugEnabled)
            logger.Debug($"Discord streaming flush failed for channel {dmChannelId}: {error.Message}");
    }

    protected override async Task ShowPreviewAsync(string text, CancellationToken token)
    {
        try
        {
            if (_currentMessageId.Length == 0)
                _currentMessageId = await discord.CreateMessageAsync(botToken, dmChannelId, text, token);
            else
                await discord.EditMessageAsync(botToken, dmChannelId, _currentMessageId, text, token);
        }
        catch (DiscordApiException e) when (e.RateLimited)
        {
            var delay = e.RetryAfter ?? TimeSpan.FromSeconds(1);
            _previewCooldownUntil = DateTime.UtcNow + (delay > MaxRetryDelay ? MaxRetryDelay : delay);
            throw;
        }
    }

    protected override Task SendFinalAsync(string text, CancellationToken token) => CreateWithRetryAsync(text, token);

    protected override Task EditFinalAsync(string text, CancellationToken token) =>
        text == LastShownText ? Task.CompletedTask : EditWithRetryAsync(_currentMessageId, text, token);

    private async Task CreateWithRetryAsync(string text, CancellationToken token)
    {
        try
        {
            await discord.CreateMessageAsync(botToken, dmChannelId, text, token);
        }
        catch (DiscordApiException e) when (e.RateLimited)
        {
            await DelayForRetryAsync(e, token);
            await discord.CreateMessageAsync(botToken, dmChannelId, text, token);
        }
    }

    private async Task EditWithRetryAsync(string messageId, string text, CancellationToken token)
    {
        try
        {
            await discord.EditMessageAsync(botToken, dmChannelId, messageId, text, token);
        }
        catch (DiscordApiException e) when (e.RateLimited)
        {
            await DelayForRetryAsync(e, token);
            await discord.EditMessageAsync(botToken, dmChannelId, messageId, text, token);
        }
    }

    private static Task DelayForRetryAsync(DiscordApiException e, CancellationToken token)
    {
        var delay = e.RetryAfter ?? TimeSpan.FromSeconds(1);
        return Task.Delay(delay > MaxRetryDelay ? MaxRetryDelay : delay, token);
    }
}
