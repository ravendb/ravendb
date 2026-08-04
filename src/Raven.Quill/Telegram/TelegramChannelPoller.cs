using Raven.Quill.Channels;
using Telegram.Bot;
using Telegram.Bot.Types;
using Telegram.Bot.Types.Enums;

namespace Raven.Quill.Telegram;

/// One long-poll loop per enabled Telegram channel: getUpdates with a 30s hold, offset confirmed after each
/// batch, exponential backoff with jitter on errors (reset by the next successful poll). Messages dispatch onto
/// per-chat task chains — strictly ordered within a chat, concurrent across chats — so polling continues while
/// the agent streams a reply. Cancelling stops within ~1s: the token aborts the pending long-poll HTTP call.
internal sealed class TelegramChannelPoller
{
    private static readonly TimeSpan MinBackoff = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan MaxBackoff = TimeSpan.FromSeconds(30);

    private readonly ITelegramBotClient _bot;
    private readonly ILogger _logger;
    private readonly CancellationTokenSource _cts = new();
    private readonly Dictionary<long, Task> _chatChains = new();
    private readonly object _chainsLock = new();
    private Task _loop = Task.CompletedTask;

    public TelegramChannelPoller(string database, Channel channel, ITelegramBotClient bot, ILogger logger)
    {
        Database = database;
        Channel = channel;
        _bot = bot;
        _logger = logger;
    }

    public string Database { get; }

    /// Snapshot of the channel doc at (re)start; edits restart the poller with a fresh snapshot.
    public Channel Channel { get; }

    public TelegramChannelHealth Health { get; } = new();

    public void Start() => _loop = Task.Run(RunLoopAsync);

    private async Task RunLoopAsync()
    {
        var ct = _cts.Token;
        var token = Channel.Telegram!.BotToken;
        int? offset = null;
        var backoff = MinBackoff;

        while (ct.IsCancellationRequested == false)
        {
            try
            {
                var updates = await _bot.GetUpdates(
                    offset: offset, timeout: 30, allowedUpdates: [UpdateType.Message], cancellationToken: ct);

                Health.RecordSuccess(DateTime.UtcNow);
                backoff = MinBackoff;

                if (updates.Length > 0)
                {
                    // confirm before dispatch: at-most-once for in-flight work, at-least-once across restarts
                    offset = updates[^1].Id + 1;
                    foreach (var update in updates)
                        Dispatch(update, ct);
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception e)
            {
                var scrubbed = TelegramSettings.ScrubToken(e.Message, token);
                Health.RecordError(DateTime.UtcNow, scrubbed);
                _logger.LogWarning(
                    "Telegram poll failed for channel {ChannelId} (bot {Bot}): {Error}",
                    Channel.Id, TelegramSettings.RedactToken(token), scrubbed);

                try
                {
                    await Task.Delay(backoff, ct);
                }
                catch (OperationCanceledException)
                {
                    break;
                }

                var doubled = backoff * 2 + TimeSpan.FromMilliseconds(Random.Shared.Next(0, 250));
                backoff = doubled < MaxBackoff ? doubled : MaxBackoff;
            }
        }
    }

    private void Dispatch(Update update, CancellationToken ct)
    {
        if (update.Message is not { Text.Length: > 0 } message)
            return;

        var chatId = message.Chat.Id;
        lock (_chainsLock)
        {
            var tail = _chatChains.GetValueOrDefault(chatId) ?? Task.CompletedTask;
            var next = tail
                .ContinueWith(_ => HandleMessageSafeAsync(message, ct), ct,
                    TaskContinuationOptions.None, TaskScheduler.Default)
                .Unwrap();
            _chatChains[chatId] = next;

            next.ContinueWith(_ =>
            {
                lock (_chainsLock)
                {
                    if (_chatChains.TryGetValue(chatId, out var current) && current == next)
                        _chatChains.Remove(chatId);
                }
            }, TaskScheduler.Default);
        }
    }

    private async Task HandleMessageSafeAsync(Message message, CancellationToken ct)
    {
        try
        {
            await HandleMessageAsync(message, ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
        }
        catch (Exception e)
        {
            var scrubbed = TelegramSettings.ScrubToken(e.Message, Channel.Telegram!.BotToken);
            Health.RecordError(DateTime.UtcNow, scrubbed);
            _logger.LogWarning(
                "Telegram message handling failed for channel {ChannelId} chat {ChatId}: {Error}",
                Channel.Id, message.Chat.Id, scrubbed);
        }
    }

    private Task HandleMessageAsync(Message message, CancellationToken ct)
    {
        // agent dispatch pipeline lands with the update-handling commit
        _logger.LogDebug("Telegram update received for channel {ChannelId} chat {ChatId}", Channel.Id, message.Chat.Id);
        return Task.CompletedTask;
    }

    /// Bounded: cancel aborts the pending long poll, then the loop and any in-flight chat chains get ~1s each.
    public async Task StopAsync()
    {
        await _cts.CancelAsync();

        try
        {
            await _loop.WaitAsync(TimeSpan.FromSeconds(1));
        }
        catch (TimeoutException)
        {
            _logger.LogWarning("Telegram poller for channel {ChannelId} did not stop within 1s", Channel.Id);
        }

        Task[] chains;
        lock (_chainsLock)
            chains = _chatChains.Values.ToArray();

        if (chains.Length > 0)
        {
            try
            {
                await Task.WhenAll(chains).WaitAsync(TimeSpan.FromSeconds(1));
            }
            catch (Exception)
            {
                // chain tasks self-log; stopping must not throw
            }
        }

        _cts.Dispose();
    }
}
