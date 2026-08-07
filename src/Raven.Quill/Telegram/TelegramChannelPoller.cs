using System.Globalization;
using Raven.Client.Documents;
using Raven.Quill.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Hosting;
using Raven.Quill.Metrics;
using Telegram.Bot;
using Telegram.Bot.Types;
using Telegram.Bot.Types.Enums;

namespace Raven.Quill.Telegram;

internal sealed class TelegramChannelPoller
{
    private static readonly TimeSpan MinBackoff = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan MaxBackoff = TimeSpan.FromSeconds(30);

    private readonly IDocumentStore _store;
    private readonly IAgentRouter _router;
    private readonly ApplianceOptions _options;
    private readonly ITelegramBotClient _bot;
    private readonly ILogger _logger;
    private readonly string _channelId;
    private readonly CancellationTokenSource _cts = new();
    private readonly Dictionary<long, Task> _chatChains = new();
    private readonly object _chainsLock = new();
    private Task _loop = Task.CompletedTask;

    public TelegramChannelPoller(
        string database, Channel channel, ITelegramBotClient bot,
        IDocumentStore store, IAgentRouter router, ApplianceOptions options, ILogger logger)
    {
        Database = database;
        Channel = channel;
        _bot = bot;
        _store = store;
        _router = router;
        _options = options;
        _logger = logger;
        _channelId = Channel.StripIdPrefix(channel.Id);
    }

    public string Database { get; }

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
                var message = e.InnerException is null ? e.Message : $"{e.Message}: {e.InnerException.Message}";
                var scrubbed = TelegramSettings.ScrubToken(message, token);
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

    private async Task HandleMessageAsync(Message message, CancellationToken ct)
    {
        var prompt = message.Text!.Trim();
        if (prompt.Length == 0)
            return;

        if (message.Chat.Type != ChatType.Private)
        {
            await SendPlainAsync(message.Chat.Id,
                "I only work in one-on-one chats. Message me directly to start a conversation.", ct);
            return;
        }

        var chatId = message.Chat.Id;
        var conversationId = TelegramConversationId.For(_channelId, chatId, DateTime.UtcNow);

        if (IsCommand(prompt, "clear"))
        {
            await ClearConversationAsync(conversationId, ct);
            await SendPlainAsync(chatId, "Conversation cleared. The next message starts a fresh one.", ct);
            return;
        }

        if (IsCommand(prompt, "start"))
        {
            await SendPlainAsync(chatId,
                "Hi! Ask me anything and I'll answer. Send /clear anytime to start a fresh conversation.", ct);
            return;
        }

        var config = await AgentLookup.FindAsync(_store, Database, Channel.AgentId, ct);
        if (config is null)
            throw new InvalidOperationException($"agent '{Channel.AgentId}' is no longer registered in this app");

        var parameters = new Dictionary<string, string>(Channel.Telegram!.Parameters);
        var identifierParameter = config.Parameters?.FirstOrDefault(p =>
            string.Equals(p.Name, TelegramSettings.TelegramUserIdentifierParameterName, StringComparison.OrdinalIgnoreCase));
        if (identifierParameter is not null &&
            parameters.Keys.Any(k => string.Equals(k, identifierParameter.Name, StringComparison.OrdinalIgnoreCase)) == false)
        {
            var userId = message.From?.Id ?? chatId;
            parameters[identifierParameter.Name] = userId.ToString(CultureInfo.InvariantCulture);
        }

        var usernameParameter = config.Parameters?.FirstOrDefault(p =>
            string.Equals(p.Name, TelegramSettings.TelegramUsernameParameterName, StringComparison.OrdinalIgnoreCase));
        if (usernameParameter is not null &&
            parameters.Keys.Any(k => string.Equals(k, usernameParameter.Name, StringComparison.OrdinalIgnoreCase)) == false)
        {
            var username = message.From?.Username;
            if (string.IsNullOrEmpty(username))
            {
                await SendPlainAsync(chatId,
                    "This assistant needs your Telegram username. Set one in Telegram Settings and send your message again.", ct);
                return;
            }

            parameters[usernameParameter.Name] = username;
        }

        var unbound = (config.Parameters ?? [])
            .Select(p => p.Name)
            .Where(name => string.IsNullOrWhiteSpace(name) == false && parameters.ContainsKey(name) == false)
            .ToArray();
        if (unbound.Length > 0)
        {
            await SendPlainAsync(chatId,
                "This assistant is not fully configured yet. Please contact whoever set up this bot.", ct);
            throw new InvalidOperationException(
                $"agent '{config.Identifier}' has unbound parameter(s): {string.Join(", ", unbound)}");
        }

        try
        {
            await _bot.SendChatAction(chatId, ChatAction.Typing, cancellationToken: ct);
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {

        }

        var reply = new TelegramStreamingReply(
            _bot, chatId, _options.TelegramEditDebounce, Channel.Telegram.BotToken, _logger, ct);

        try
        {
            var result = await _router.RunAsync(
                new AgentRequest(Database, config.Identifier, conversationId, prompt, Channel.Id!, parameters),
                reply.OnChunkAsync, config, ct);

            var fullReply = string.IsNullOrWhiteSpace(result.Reply) ? reply.AccumulatedText : result.Reply;
            await reply.FinalizeAsync(fullReply);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch
        {
            await TrySendPlainAsync(chatId, "Sorry - something went wrong handling that message. Please try again.", ct);
            throw;
        }
    }

    // matches Telegram's "/name[@botUsername] [payload]" command shape
    private bool IsCommand(string text, string name)
    {
        var separator = text.IndexOfAny([' ', '\t', '\r', '\n']);
        var command = separator < 0 ? text : text[..separator];

        if (command.Equals($"/{name}", StringComparison.OrdinalIgnoreCase))
            return true;

        var username = Channel.Telegram?.BotUsername;
        return string.IsNullOrEmpty(username) == false &&
               command.Equals($"/{name}@{username}", StringComparison.OrdinalIgnoreCase);
    }

    private async Task ClearConversationAsync(string conversationId, CancellationToken ct)
    {
        using var session = _store.OpenAsyncSession(Database);
        session.Delete(conversationId);
        session.Delete(ConversationPreview.IdFor(conversationId));
        await session.SaveChangesAsync(ct);
    }

    private Task SendPlainAsync(long chatId, string text, CancellationToken ct) =>
        _bot.SendMessage(chatId, text, cancellationToken: ct);

    private async Task TrySendPlainAsync(long chatId, string text, CancellationToken ct)
    {
        try
        {
            await SendPlainAsync(chatId, text, ct);
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {

        }
    }

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
