using Discord;
using Raven.Quill.Logging;
using Raven.Quill.Channels;
using Raven.Quill.Hosting;

namespace Raven.Quill.Discord;

internal sealed class DiscordStreamingReply(
    IMessageChannel channel,
    string dmChannelId,
    DiscordSdk sdk,
    DiscordOptions options,
    QuillLogger<DiscordRuntime> logger) : ChannelStreamingReply(options.MessageLimit, options.EditDebounce)
{
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
        if (_currentMessageId.Length == 0)
            _currentMessageId = await channel.CreateAsync(sdk, text, suppressEmbeds: true, token);
        else
            await channel.EditAsync(sdk, _currentMessageId, text, suppressEmbeds: true, token);
    }

    protected override Task SendFinalAsync(string text, CancellationToken token) =>
        channel.CreateAsync(sdk, text, suppressEmbeds: false, token);

    protected override Task EditFinalAsync(string text, CancellationToken token) =>
        channel.EditAsync(sdk, _currentMessageId, text, suppressEmbeds: false, token);
}
