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

internal sealed class TelegramBotRuntime
{
    private const string GetUpdatesMethod = "getUpdates";

    private static readonly TimeSpan MinBackoff = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan MaxBackoff = TimeSpan.FromSeconds(30);

    private readonly ConcurrentDictionary<long, TelegramChat> _chats = new();
    private readonly CancellationTokenSource _cts = new();
    private readonly TelegramChatContext _context;
    private readonly bool _acceptsContactShares;
    private Task _receive = Task.CompletedTask;
    private TimeSpan _backoff = MinBackoff;

    private TelegramBotRuntime(
        string database, Channel channel, ITelegramBotClient client,
        IDocumentStore store, IAgentRouter router, ApplianceOptions options, ILogger logger)
    {
        Client = client;
        BotToken = channel.Telegram!.BotToken;
        _acceptsContactShares = channel.Telegram.ParameterBindings.Values
            .Any(binding => binding.Source == TelegramParameterSource.PhoneNumber);
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
            Health.RecordSuccess(DateTime.UtcNow);
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
            _ = TrySendPlainAsync(message.Chat.Id,
                "I only work in one-on-one chats. Message me directly to start a conversation.");
            return Task.CompletedTask;
        }

        var chat = _chats.GetOrAdd(message.Chat.Id, id => new TelegramChat(id, this, _context, _cts.Token));

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
            _context.Logger.LogWarning(
                "Telegram bot {Bot} did not drain within 10s", TelegramSettings.RedactToken(BotToken));
            return;
        }
        catch (Exception)
        {
        }

        _cts.Dispose();
    }
}

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
