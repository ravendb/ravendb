using Raven.Quill.Channels;
using Raven.Quill.Hosting;
using Telegram.Bot;
using Telegram.Bot.Exceptions;
using Telegram.Bot.Types.Enums;

using Raven.Quill.Logging;

namespace Raven.Quill.Telegram;

internal sealed class TelegramStreamingReply(
    ITelegramBotClient bot,
    long chatId,
    TelegramOptions options,
    QuillLogger<TelegramChannelManager> logger) : ChannelStreamingReply(options.MessageLimit, options.EditDebounce)
{
    private int _currentMessageId;

    protected override bool HasOpenMessage => _currentMessageId != 0;

    protected override void CloseCurrentMessage() => _currentMessageId = 0;

    protected override void LogFlushFailure(Exception error)
    {
        if (logger.IsDebugEnabled)
            logger.Debug($"Telegram streaming flush failed for chat {chatId}: {error.Message}");
    }

    protected override async Task ShowPreviewAsync(string text, CancellationToken token)
    {
        if (_currentMessageId == 0)
        {
            var message = await bot.SendMessage(chatId, text, cancellationToken: token);
            _currentMessageId = message.Id;
        }
        else
        {
            await EditPlainAsync(_currentMessageId, text, token);
        }
    }

    protected override async Task SendFinalAsync(string text, CancellationToken token)
    {
        try
        {
            await bot.SendMessage(chatId, text, parseMode: ParseMode.Markdown, cancellationToken: token);
        }
        catch (ApiRequestException)
        {
            await bot.SendMessage(chatId, text, cancellationToken: token);
        }
    }

    protected override async Task EditFinalAsync(string text, CancellationToken token)
    {
        try
        {
            await bot.EditMessageText(chatId, _currentMessageId, text, parseMode: ParseMode.Markdown, cancellationToken: token);
        }
        catch (ApiRequestException e) when (IsNotModified(e))
        {
        }
        catch (ApiRequestException)
        {
            await EditPlainAsync(_currentMessageId, text, token);
        }
    }

    private async Task EditPlainAsync(int messageId, string text, CancellationToken token)
    {
        try
        {
            await bot.EditMessageText(chatId, messageId, text, cancellationToken: token);
        }
        catch (ApiRequestException e) when (IsNotModified(e))
        {
        }
    }

    private static bool IsNotModified(ApiRequestException e) =>
        e.Message.Contains("message is not modified", StringComparison.OrdinalIgnoreCase);
}
