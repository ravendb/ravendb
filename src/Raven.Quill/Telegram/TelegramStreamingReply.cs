using System.Text;
using Raven.Quill.Channels;
using Telegram.Bot;
using Telegram.Bot.Exceptions;
using Telegram.Bot.Types.Enums;

namespace Raven.Quill.Telegram;

internal sealed class TelegramStreamingReply(
    ITelegramBotClient bot,
    long chatId,
    TimeSpan editDebounce,
    string botToken,
    ILogger logger,
    CancellationToken ct)
{
    private readonly StringBuilder _buffer = new();
    private readonly List<int> _messageIds = [];
    private int _currentMessageId;
    private int _flushedUpTo;
    private string _lastPreviewText = "";
    private DateTime _lastFlushAt;

    public string AccumulatedText => _buffer.ToString();

    private int PendingLength => _buffer.Length - _flushedUpTo;

    public async ValueTask OnChunkAsync(string chunk)
    {
        _buffer.Append(chunk);

        if (PendingLength <= TelegramMessageSplitter.TelegramMessageLimit &&
            DateTime.UtcNow - _lastFlushAt < editDebounce)
            return;

        try
        {
            await FlushPreviewAsync();
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            logger.LogDebug("Telegram streaming flush failed for chat {ChatId}: {Error}",
                chatId, TelegramSettings.ScrubToken(e.Message, botToken));
        }
        finally
        {
            _lastFlushAt = DateTime.UtcNow;
        }
    }

    private async Task FlushPreviewAsync()
    {
        while (PendingLength > TelegramMessageSplitter.TelegramMessageLimit)
        {
            var cut = TelegramMessageSplitter.TelegramMessageLimit;
            if (char.IsHighSurrogate(_buffer[_flushedUpTo + cut - 1]))
                cut--;

            await ShowPreviewAsync(_buffer.ToString(_flushedUpTo, cut));
            _flushedUpTo += cut;
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
            _messageIds.Add(message.Id);
        }
        else
        {
            await EditPlainAsync(_currentMessageId, text);
        }

        _lastPreviewText = text;
    }

    public async Task FinalizeAsync(string fullReply)
    {
        if (string.IsNullOrWhiteSpace(fullReply))
            return;

        var parts = TelegramMessageSplitter.Split(fullReply);

        for (var i = 0; i < parts.Count; i++)
        {
            if (i < _messageIds.Count)
                await EditSafeAsync(_messageIds[i], parts[i]);
            else
                await SendSafeAsync(parts[i]);
        }

        for (var i = parts.Count; i < _messageIds.Count; i++)
            await DeleteSafeAsync(_messageIds[i]);
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

    private async Task DeleteSafeAsync(int messageId)
    {
        try
        {
            await bot.DeleteMessage(chatId, messageId, ct);
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            logger.LogDebug("Telegram preview delete failed for chat {ChatId}: {Error}",
                chatId, TelegramSettings.ScrubToken(e.Message, botToken));
        }
    }

    private static bool IsNotModified(ApiRequestException e) =>
        e.Message.Contains("message is not modified", StringComparison.OrdinalIgnoreCase);
}
