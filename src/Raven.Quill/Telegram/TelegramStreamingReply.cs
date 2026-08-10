using System.Text;
using Raven.Quill.Channels;
using Telegram.Bot;
using Telegram.Bot.Exceptions;
using Telegram.Bot.Types;
using Telegram.Bot.Types.Enums;

namespace Raven.Quill.Telegram;

/// Per-turn streaming state machine (prototype pattern): the first flush sends a message, later flushes edit it
/// in place. A flush is skipped while an edit is in flight, inside the debounce window (Telegram rate-limits
/// edits), or when the text is unchanged - chunks keep accumulating and the next trigger catches up.
/// <see cref="FinalizeAsync"/> lands the authoritative reply: it edits the message to the first split part and
/// sends the 4096-overflow as follow-up messages.
internal sealed class TelegramStreamingReply(
    ITelegramBotClient bot,
    long chatId,
    TimeSpan editDebounce,
    string botToken,
    ILogger logger,
    CancellationToken ct)
{
    private readonly StringBuilder _buffer = new();
    private readonly object _lock = new();
    private Task _pendingFlush = Task.CompletedTask;
    private bool _flushRunning;
    private DateTime _lastFlushAt;
    private int _messageId;
    private string _lastSentText = "";
    private bool _lastSentMarkdown;

    /// The streamed text accumulated so far — the fallback reply when the router yields no extracted text.
    public string AccumulatedText
    {
        get { lock (_lock) return _buffer.ToString(); }
    }

    public ValueTask OnChunkAsync(string chunk)
    {
        lock (_lock)
        {
            _buffer.Append(chunk);

            if (_flushRunning || DateTime.UtcNow - _lastFlushAt < editDebounce)
                return ValueTask.CompletedTask;   // a later chunk or FinalizeAsync catches up

            _flushRunning = true;
            _pendingFlush = Task.Run(FlushOnceAsync, CancellationToken.None);
        }

        return ValueTask.CompletedTask;
    }

    private async Task FlushOnceAsync()
    {
        try
        {
            string text;
            lock (_lock)
                text = _buffer.ToString();

            // stream preview caps at the message limit; FinalizeAsync splits the real overflow
            if (text.Length > TelegramMessageSplitter.TelegramMessageLimit)
                text = text[..TelegramMessageSplitter.TelegramMessageLimit];

            await SendOrEditAsync(text, markdown: false);
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            // a failed preview edit must not kill the run; the finalize pass retries with the full reply
            logger.LogDebug("Telegram streaming flush failed for chat {ChatId}: {Error}",
                chatId, TelegramSettings.ScrubToken(e.Message, botToken));
        }
        finally
        {
            lock (_lock)
            {
                _flushRunning = false;
                _lastFlushAt = DateTime.UtcNow;
            }
        }
    }

    public async Task FinalizeAsync(string fullReply)
    {
        Task pending;
        lock (_lock)
            pending = _pendingFlush;

        try
        {
            await pending;
        }
        catch
        {
            // flush failures already logged; the authoritative edit below decides the outcome
        }

        if (string.IsNullOrWhiteSpace(fullReply))
            return;   // nothing to land; any streamed preview stays as-is

        var parts = TelegramMessageSplitter.Split(fullReply);
        await SendOrEditAsync(parts[0], markdown: true);
        for (var i = 1; i < parts.Count; i++)
            await SendSafeAsync(parts[i], markdown: true);
    }

    private async Task SendOrEditAsync(string text, bool markdown)
    {
        if (text.Length == 0 || (text == _lastSentText && markdown == _lastSentMarkdown))
            return;

        if (_messageId == 0)
        {
            var message = await SendSafeAsync(text, markdown);
            _messageId = message.Id;
        }
        else
        {
            await EditSafeAsync(_messageId, text, markdown);
        }

        _lastSentText = text;
        _lastSentMarkdown = markdown;
    }

    private async Task<Message> SendSafeAsync(string text, bool markdown)
    {
        if (markdown == false)
            return await bot.SendMessage(chatId, text, cancellationToken: ct);

        try
        {
            return await bot.SendMessage(chatId, text, parseMode: ParseMode.Markdown, cancellationToken: ct);
        }
        catch (ApiRequestException)
        {
            return await bot.SendMessage(chatId, text, cancellationToken: ct);
        }
    }

    private async Task EditSafeAsync(int messageId, string text, bool markdown)
    {
        if (markdown == false)
        {
            await EditPlainAsync(messageId, text);
            return;
        }

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
