using System.Collections.Concurrent;
using Raven.Client.Documents;
using Raven.Quill.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Hosting;
using Telegram.Bot;
using Telegram.Bot.Args;
using Telegram.Bot.Polling;
using Telegram.Bot.Types;
using Telegram.Bot.Types.Enums;

namespace Raven.Quill.Telegram;

/// Everything one enabled Telegram channel needs at runtime: the bot client, the long-poll receiver, the
/// chat registry, health counters and the poll-error backoff. Created and destroyed only by
/// <see cref="TelegramChannelManager"/>; stopping it takes its chats down with it.
internal sealed class TelegramBotRuntime
{
    private const string GetUpdatesMethod = "getUpdates";

    private static readonly TimeSpan MinBackoff = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan MaxBackoff = TimeSpan.FromSeconds(30);

    private readonly ConcurrentDictionary<long, TelegramChat> _chats = new();
    private readonly CancellationTokenSource _cts = new();
    private readonly TelegramChatContext _context;
    private Task _receive = Task.CompletedTask;
    private TimeSpan _backoff = MinBackoff;

    private TelegramBotRuntime(
        string database, Channel channel, ITelegramBotClient client,
        IDocumentStore store, IAgentRouter router, ApplianceOptions options, ILogger logger)
    {
        Client = client;
        BotToken = channel.Telegram!.BotToken;
        _context = new TelegramChatContext(database, channel, store, router, options, logger);
    }

    public ITelegramBotClient Client { get; }

    public string BotToken { get; }

    public TelegramChannelHealth Health { get; } = new();

    public static TelegramBotRuntime Start(
        string database, Channel channel, ITelegramBotClientFactory botFactory,
        IDocumentStore store, IAgentRouter router, ApplianceOptions options, ILogger logger)
    {
        var runtime = new TelegramBotRuntime(
            database, channel, botFactory.Create(channel.Telegram!.BotToken), store, router, options, logger);

        runtime.Run();
        return runtime;
    }

    private void Run()
    {
        // DropPendingUpdates stays false: messages sent while the bot was down are still delivered, which
        // is what the hand-rolled loop this replaced did. HandleErrorSource is only reachable through
        // IUpdateHandler, so the receiver goes through DefaultUpdateHandler rather than the Func overload.
        var handler = new DefaultUpdateHandler(OnUpdate, OnErrorAsync);
        var receiverOptions = new ReceiverOptions
        {
            AllowedUpdates = [UpdateType.Message],
            DropPendingUpdates = false,
        };

        // the receiver reports errors and updates but never a poll that simply came back empty, so the
        // "still reachable" half of health is read off the HTTP responses instead
        Client.OnApiResponseReceived += OnApiResponseReceived;

        _receive = Client.ReceiveAsync(handler, receiverOptions, _cts.Token);
    }

    private ValueTask OnApiResponseReceived(
        ITelegramBotClient client, ApiResponseEventArgs args, CancellationToken ct)
    {
        if (args.ApiRequestEventArgs.Request.MethodName == GetUpdatesMethod &&
            args.ResponseMessage.IsSuccessStatusCode)
        {
            Health.RecordSuccess(DateTime.UtcNow);
            _backoff = MinBackoff;
        }

        return default;
    }

    /// The receiver awaits this before issuing the next getUpdates, so it holds a message and therefore
    /// awaits nothing: it routes or it drops.
    private Task OnUpdate(ITelegramBotClient client, Update update, CancellationToken ct)
    {
        if (update.Message is not { Text.Length: > 0 } message)
            return Task.CompletedTask;

        if (message.Chat.Type != ChatType.Private)
        {
            // fire-and-forget: a polite refusal must never delay this bot's intake, and a group chat
            // gets no queue of its own
            _ = TrySendPlainAsync(message.Chat.Id,
                "I only work in one-on-one chats. Message me directly to start a conversation.");
            return Task.CompletedTask;
        }

        var chat = _chats.GetOrAdd(message.Chat.Id, id => new TelegramChat(id, this, _context, _cts.Token));

        // a chat can never be dead while its bot lives, so a refused post means exactly one thing
        if (chat.TryPost(message) == false)
        {
            Health.RecordError(DateTime.UtcNow, $"chat {message.Chat.Id}: queue full, message dropped");
            chat.NotifyOverloadOnce();
        }

        return Task.CompletedTask;
    }

    private async Task OnErrorAsync(
        ITelegramBotClient client, Exception e, HandleErrorSource source, CancellationToken ct)
    {
        var message = e.InnerException is null ? e.Message : $"{e.Message}: {e.InnerException.Message}";
        var scrubbed = TelegramSettings.ScrubToken(message, BotToken);

        Health.RecordError(DateTime.UtcNow, scrubbed);
        _context.Logger.LogWarning(
            "Telegram poll failed for channel {ChannelId} (bot {Bot}): {Error}",
            _context.ChannelDoc.Id, TelegramSettings.RedactToken(BotToken), scrubbed);

        if (source != HandleErrorSource.PollingError)
            return;

        // getUpdates itself failed, so no messages are arriving and the library would otherwise re-poll
        // in a hot loop. this delay is the retry throttle; it blocks nothing, because there is nothing
        // left to block.
        try
        {
            await Task.Delay(_backoff, ct);
        }
        catch (OperationCanceledException)
        {
            return;
        }

        var doubled = _backoff * 2 + TimeSpan.FromMilliseconds(Random.Shared.Next(0, 250));
        _backoff = doubled < MaxBackoff ? doubled : MaxBackoff;
    }

    internal async Task TrySendPlainAsync(long chatId, string text)
    {
        try
        {
            await Client.SendMessage(chatId, text, cancellationToken: _cts.Token);
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            Health.RecordError(DateTime.UtcNow, TelegramSettings.ScrubToken(e.Message, BotToken));
        }
    }

    public async Task StopAsync()
    {
        Client.OnApiResponseReceived -= OnApiResponseReceived;
        await _cts.CancelAsync();

        var draining = _chats.Values.Select(c => c.Completion).Append(_receive);

        try
        {
            await Task.WhenAll(draining).WaitAsync(TimeSpan.FromSeconds(10));
        }
        catch (TimeoutException)
        {
            // leak the CTS rather than dispose it under a task that still reads its token
            _context.Logger.LogWarning(
                "Telegram bot {Bot} did not drain within 10s", TelegramSettings.RedactToken(BotToken));
            return;
        }
        catch (Exception)
        {
            // the receiver unwinds through the cancelled token, and chat loops log their own failures;
            // either way every task has finished, so the token is free
        }

        _cts.Dispose();
    }
}

/// The per-chat slice of its bot's configuration. The channel document is captured once, so a bot restart
/// is what picks up an edited AgentId or Parameters.
internal sealed class TelegramChatContext
{
    public TelegramChatContext(
        string database, Channel channel,
        IDocumentStore store, IAgentRouter router, ApplianceOptions options, ILogger logger)
    {
        Database = database;
        ChannelDoc = channel;
        ChannelId = Channel.StripIdPrefix(channel.Id);
        Store = store;
        Router = router;
        Options = options;
        Logger = logger;
    }

    public string Database { get; }

    public Channel ChannelDoc { get; }

    public string ChannelId { get; }

    public IDocumentStore Store { get; }

    public IAgentRouter Router { get; }

    public ApplianceOptions Options { get; }

    public ILogger Logger { get; }
}
