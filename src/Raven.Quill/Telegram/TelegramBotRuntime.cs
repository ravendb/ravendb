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

using Raven.Quill.Logging;

namespace Raven.Quill.Telegram;

internal sealed class TelegramBotRuntime
{
    private const string GetUpdatesMethod = "getUpdates";

    private static readonly TimeSpan MinBackoff = TimeSpan.FromSeconds(1);

    private readonly ConcurrentDictionary<long, TelegramChat> _chats = new();
    private readonly ConcurrentDictionary<long, bool> _refusedGroupChats = new();
    private readonly CancellationTokenSource _cts = new();
    private readonly TelegramChatContext _context;
    private readonly bool _acceptsContactShares;
    private Task _receive = Task.CompletedTask;
    private TimeSpan _backoff = MinBackoff;

    private TelegramBotRuntime(
        string database, Channel channel, string? channelChangeVector, ITelegramBotClient client,
        IDocumentStore store, IAgentRouter router, ApplianceOptions options,
        QuillLogger<TelegramChannelManager> logger)
    {
        Client = client;
        ChannelChangeVector = channelChangeVector;
        _acceptsContactShares = channel.Telegram!.ParameterBindings.Values
            .Any(binding => binding.Source == ChannelParameterSource.PhoneNumber);
        _context = new TelegramChatContext(database, channel, store, router, options, logger);
    }

    public ITelegramBotClient Client { get; }

    /// The channel document's change vector at start; the manager restarts the bot when it moves.
    public string? ChannelChangeVector { get; }

    public static TelegramBotRuntime Start(
        string database, Channel channel, string? channelChangeVector, ITelegramBotClientFactory botFactory,
        IDocumentStore store, IAgentRouter router, ApplianceOptions options,
        QuillLogger<TelegramChannelManager> logger)
    {
        var runtime = new TelegramBotRuntime(
            database, channel, channelChangeVector, botFactory.Create(channel.Telegram!.BotToken),
            store, router, options, logger);

        runtime.Run();
        return runtime;
    }

    private void Run()
    {
        var handler = new DefaultUpdateHandler(OnUpdate, OnErrorAsync);
        var receiverOptions = new ReceiverOptions
        {
            AllowedUpdates = [UpdateType.Message],
            DropPendingUpdates = false,
        };

        Client.OnApiResponseReceived += OnApiResponseReceived;

        _receive = Client.ReceiveAsync(handler, receiverOptions, _cts.Token);
    }

    private ValueTask OnApiResponseReceived(
        ITelegramBotClient client, ApiResponseEventArgs args, CancellationToken ct)
    {
        if (args.ApiRequestEventArgs.Request.MethodName == GetUpdatesMethod &&
            args.ResponseMessage.IsSuccessStatusCode)
        {
            _backoff = MinBackoff;
        }

        return default;
    }

    private Task OnUpdate(ITelegramBotClient client, Update update, CancellationToken ct)
    {
        if (update.Message is not { } message)
            return Task.CompletedTask;

        var hasText = message.Text is { Length: > 0 };
        var isContactShare = _acceptsContactShares && message.Contact is not null;
        if (hasText == false && isContactShare == false)
            return Task.CompletedTask;

        if (message.Chat.Type != ChatType.Private)
        {
            // one refusal per group chat per bot runtime, so a busy group is never spammed
            if (_refusedGroupChats.TryAdd(message.Chat.Id, true))
                _ = TrySendPlainAsync(message.Chat.Id, _context.Messages.GroupChatRefusal);
            return Task.CompletedTask;
        }

        while (true)
        {
            var chat = _chats.GetOrAdd(message.Chat.Id, id => new TelegramChat(id, this, _context, _cts.Token));

            if (chat.TryPost(message))
                break;

            if (chat.IsRetired)
            {
                // the idle loop retired this instance between lookup and post; replace it and repost
                OnChatRetired(message.Chat.Id, chat);
                continue;
            }

            _context.Logger.Warn(
                $"Telegram chat {message.Chat.Id} on channel {_context.ChannelDoc.Id} dropped a " +
                "message: queue full");
            chat.NotifyOverloadOnce();
            break;
        }

        return Task.CompletedTask;
    }

    internal int ActiveChatCount => _chats.Count;

    internal void OnChatRetired(long chatId, TelegramChat chat) =>
        _chats.TryRemove(new KeyValuePair<long, TelegramChat>(chatId, chat));

    private async Task OnErrorAsync(
        ITelegramBotClient client, Exception e, HandleErrorSource source, CancellationToken ct)
    {
        var message = e.InnerException is null ? e.Message : $"{e.Message}: {e.InnerException.Message}";

        _context.Logger.Warn(
            $"Telegram poll failed for channel {_context.ChannelDoc.Id} " +
            $"(bot @{_context.ChannelDoc.Telegram?.BotUsername}): {message}");

        if (source != HandleErrorSource.PollingError)
            return;

        try
        {
            await Task.Delay(_backoff, ct);
        }
        catch (OperationCanceledException)
        {
            return;
        }

        var max = _context.Options.Telegram.PollBackoffMax;
        var doubled = _backoff * 2 + TimeSpan.FromMilliseconds(Random.Shared.Next(0, 250));
        _backoff = doubled < max ? doubled : max;
    }

    internal async Task TrySendPlainAsync(long chatId, string text)
    {
        try
        {
            await Client.SendMessage(chatId, text, cancellationToken: _cts.Token);
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            _context.Logger.Debug(
                $"Telegram send failed for chat {chatId} on channel {_context.ChannelDoc.Id}: " +
                $"{e.Message}");
        }
    }

    public async Task StopAsync()
    {
        Client.OnApiResponseReceived -= OnApiResponseReceived;
        await _cts.CancelAsync();

        try
        {
            await DrainAsync().WaitAsync(TimeSpan.FromSeconds(10));
        }
        catch (TimeoutException)
        {
            _context.Logger.Warn(
                $"Telegram bot for channel {_context.ChannelDoc.Id} did not drain within 10s");
            return;
        }
        catch (OperationCanceledException)
        {
        }

        _cts.Dispose();
    }

    private async Task DrainAsync()
    {
        try
        {
            await _receive;
        }
        catch (OperationCanceledException)
        {
        }

        await Task.WhenAll(_chats.Values.Select(c => c.Completion));
    }
}

internal sealed class TelegramChatContext
{
    public TelegramChatContext(
        string database, Channel channel,
        IDocumentStore store, IAgentRouter router, ApplianceOptions options,
        QuillLogger<TelegramChannelManager> logger)
    {
        Database = database;
        ChannelDoc = channel;
        Messages = ResolvedTelegramMessages.Resolve(channel.Telegram?.Messages);
        Store = store;
        Router = router;
        Options = options;
        Logger = logger;
    }

    public string Database { get; }

    public Channel ChannelDoc { get; }

    public ResolvedTelegramMessages Messages { get; }

    public IDocumentStore Store { get; }

    public IAgentRouter Router { get; }

    public ApplianceOptions Options { get; }

    public QuillLogger<TelegramChannelManager> Logger { get; }
}
