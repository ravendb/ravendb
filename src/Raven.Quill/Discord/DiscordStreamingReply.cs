using Raven.Quill.Logging;
using Raven.Quill.Channels;
using Raven.Quill.Hosting;

namespace Raven.Quill.Discord;

internal sealed class DiscordStreamingReply(
    DiscordBotClient bot,
    string dmChannelId,
    DiscordOptions options,
    QuillLogger<DiscordRuntime> logger) : ChannelStreamingReply(options.MessageLimit, options.EditDebounce)
{
    private DiscordDmChannel? _channel;
    private string _currentMessageId = "";

    protected override bool HasOpenMessage => _currentMessageId.Length > 0;

    protected override void CloseCurrentMessage() => _currentMessageId = "";

    protected override void LogFlushFailure(Exception error)
    {
        if (logger.IsDebugEnabled)
            logger.Debug($"Discord streaming flush failed for channel {dmChannelId}: {error.Message}");
    }

    protected override async Task ShowPreviewAsync(string text, CancellationToken token)
    {
        var channel = await ChannelAsync(token);
        if (_currentMessageId.Length == 0)
            _currentMessageId = await channel.CreateMessageAsync(text, suppressEmbeds: true, token);
        else
            await channel.EditMessageAsync(_currentMessageId, text, suppressEmbeds: true, token);
    }

    protected override async Task SendFinalAsync(string text, CancellationToken token) =>
        await (await ChannelAsync(token)).CreateMessageAsync(text, suppressEmbeds: false, token);

    protected override async Task EditFinalAsync(string text, CancellationToken token) =>
        await (await ChannelAsync(token)).EditMessageAsync(_currentMessageId, text, suppressEmbeds: false, token);

    private async Task<DiscordDmChannel> ChannelAsync(CancellationToken token) =>
        _channel ??= await bot.ChannelAsync(dmChannelId, token);
}
