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
    CancellationToken ct) : ChannelStreamingReply(options.MessageLimit, options.EditDebounce), IDisposable
{
    private string _currentMessageId = "";
    private IDisposable? _typing;

    protected override bool HasOpenMessage => _currentMessageId.Length > 0;

    public async Task BeginTypingAsync()
    {
        try
        {
            _typing = await discord.BeginTypingAsync(botToken, dmChannelId, ct);
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            if (logger.IsDebugEnabled)
                logger.Debug($"Discord typing indicator failed for channel {dmChannelId}: {e.Message}");
        }
    }

    public void Dispose() => Interlocked.Exchange(ref _typing, null)?.Dispose();

    protected override void CloseCurrentMessage() => _currentMessageId = "";

    protected override void LogFlushFailure(Exception error)
    {
        if (logger.IsDebugEnabled)
            logger.Debug($"Discord streaming flush failed for channel {dmChannelId}: {error.Message}");
    }

    protected override async Task ShowPreviewAsync(string text)
    {
        if (_currentMessageId.Length == 0)
        {
            Dispose();
            _currentMessageId = await discord.CreateMessageAsync(botToken, dmChannelId, text, suppressEmbeds: true, ct);
        }
        else
            await discord.EditMessageAsync(botToken, dmChannelId, _currentMessageId, text, suppressEmbeds: true, ct);
    }

    protected override Task SendFinalAsync(string text)
    {
        Dispose();
        return discord.CreateMessageAsync(botToken, dmChannelId, text, suppressEmbeds: false, ct);
    }

    protected override Task EditFinalAsync(string text) =>
        discord.EditMessageAsync(botToken, dmChannelId, _currentMessageId, text, suppressEmbeds: false, ct);
}
