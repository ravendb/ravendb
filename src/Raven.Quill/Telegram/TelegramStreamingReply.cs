using System.Text;
using Raven.Quill.Hosting;
using Telegram.Bot;
using Telegram.Bot.Exceptions;
using Telegram.Bot.Types.Enums;

namespace Raven.Quill.Telegram;

internal sealed class TelegramStreamingReply(
    ITelegramBotClient bot,
    long chatId,
    TelegramOptions options,
    ILogger logger,
    CancellationToken ct)
{
    private readonly StringBuilder _buffer = new();
    private int _currentMessageId;
    private int _flushedUpTo;
    private string _lastPreviewText = "";
    private DateTime _lastFlushAt;

    private int PendingLength => _buffer.Length - _flushedUpTo;

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
            logger.LogDebug("Telegram streaming flush failed for chat {ChatId}: {Error}", chatId, e.Message);
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

            await ShowPreviewAsync(pending[..cut].TrimEnd());
            _flushedUpTo += cut;
            while (PendingLength > 0 && char.IsWhiteSpace(_buffer[_flushedUpTo]))
                _flushedUpTo++;

            _currentMessageId = 0;
            _lastPreviewText = "";
        }

        await ShowPreviewAsync(_buffer.ToString(_flushedUpTo, PendingLength));
    }

    private async Task ShowPreviewAsync(string text)
    {
        if (text.Length == 0 || text == _lastPreviewText)
            return;

        if (_currentMessageId == 0)
        {
            var message = await bot.SendMessage(chatId, text, cancellationToken: ct);
            _currentMessageId = message.Id;
        }
        else
        {
            await EditPlainAsync(_currentMessageId, text);
        }

        _lastPreviewText = text;
    }

    public async Task FinalizeAsync()
    {
        var pending = _buffer.ToString(_flushedUpTo, PendingLength);
        if (string.IsNullOrWhiteSpace(pending))
            return;

        var parts = TelegramMessageSplitter.Split(pending, options.MessageLimit);
        for (var i = 0; i < parts.Count; i++)
        {
            if (i == 0 && _currentMessageId != 0)
                await EditSafeAsync(_currentMessageId, parts[i]);
            else
                await SendSafeAsync(parts[i]);
        }
    }

    private async Task SendSafeAsync(string text)
    {
        try
        {
            await bot.SendMessage(chatId, text, parseMode: ParseMode.Markdown, cancellationToken: ct);
        }
        catch (ApiRequestException)
        {
            await bot.SendMessage(chatId, text, cancellationToken: ct);
        }
    }

    private async Task EditSafeAsync(int messageId, string text)
    {
        try
        {
            await bot.EditMessageText(chatId, messageId, text, parseMode: ParseMode.Markdown, cancellationToken: ct);
        }
        catch (ApiRequestException e) when (IsNotModified(e))
        {
        }
        catch (ApiRequestException)
        {
            await EditPlainAsync(messageId, text);
        }
    }

    private async Task EditPlainAsync(int messageId, string text)
    {
        try
        {
            await bot.EditMessageText(chatId, messageId, text, cancellationToken: ct);
        }
        catch (ApiRequestException e) when (IsNotModified(e))
        {
        }
    }

    private static bool IsNotModified(ApiRequestException e) =>
        e.Message.Contains("message is not modified", StringComparison.OrdinalIgnoreCase);
}
