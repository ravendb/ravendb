using System.Collections.Concurrent;
using Raven.Quill.Channels;
using Raven.Quill.Hosting;
using Telegram.Bot;
using Telegram.Bot.Args;
using Telegram.Bot.Polling;
using Telegram.Bot.Types;
using Telegram.Bot.Types.Enums;

using Raven.Quill.Logging;

namespace Raven.Quill.Telegram;

internal sealed class TelegramRuntime : IChannelRuntime
{
    private const string GetUpdatesMethod = "getUpdates";

    private static readonly TimeSpan MinBackoff = TimeSpan.FromSeconds(1);

    private readonly ConcurrentDictionary<long, bool> _refusedGroupChats = new();
    private readonly CancellationTokenSource _cts = new();
    private readonly string _database;
    private readonly Channel _channel;
    private readonly string _groupChatRefusal;
    private readonly ChannelChats<TelegramMessage> _chats;
    private readonly TelegramOptions _options;
    private readonly QuillLogger<TelegramRuntime> _logger;
    private readonly bool _acceptsContactShares;
    private Task _receive = Task.CompletedTask;
    private TimeSpan _backoff = MinBackoff;

    private TelegramRuntime(
        string database, Channel channel, string? channelChangeVector, ITelegramBotClient client,
        ChannelChats<TelegramMessage> chats, TelegramOptions options, QuillLogger<TelegramRuntime> logger)
    {
        Client = client;
        ChannelChangeVector = channelChangeVector;
        _database = database;
        _channel = channel;
        _groupChatRefusal = ResolvedTelegramMessages.Resolve(channel.Telegram!.Messages).GroupChatRefusal;
        _chats = chats;
        _options = options;
        _logger = logger;
        _acceptsContactShares = channel.Telegram.ParameterBindings.Values
            .Any(binding => binding.Source == ChannelParameterSource.PhoneNumber);
    }

    public ITelegramBotClient Client { get; }

    /// The channel document's change vector at start; the manager restarts the bot when it moves.
    public string? ChannelChangeVector { get; }

    public static TelegramRuntime Start(
        string database, Channel channel, string? channelChangeVector, TelegramBotClientFactory botFactory,
        ChannelChats<TelegramMessage> chats, TelegramOptions options, QuillLogger<TelegramRuntime> logger)
    {
        var runtime = new TelegramRuntime(
            database, channel, channelChangeVector, botFactory.Create(channel.Telegram!.BotToken),
            chats, options, logger);

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
                _ = TrySendPlainAsync(message.Chat.Id, _groupChatRefusal);
            return Task.CompletedTask;
        }

        _chats.Enqueue(new TelegramMessage(_database, _channel, Client, message));
        return Task.CompletedTask;
    }

    private async Task OnErrorAsync(
        ITelegramBotClient client, Exception e, HandleErrorSource source, CancellationToken ct)
    {
        var message = e.InnerException is null ? e.Message : $"{e.Message}: {e.InnerException.Message}";

        _logger.Warn(
            $"Telegram poll failed for channel {_channel.Id} " +
            $"(bot @{_channel.Telegram?.BotUsername}): {message}");

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

        var max = _options.PollBackoffMax;
        var doubled = _backoff * 2 + TimeSpan.FromMilliseconds(Random.Shared.Next(0, 250));
        _backoff = doubled < max ? doubled : max;
    }

    private async Task TrySendPlainAsync(long chatId, string text)
    {
        try
        {
            await Client.SendMessage(chatId, text, cancellationToken: _cts.Token);
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            _logger.Debug(
                $"Telegram send failed for chat {chatId} on channel {_channel.Id}: " +
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
            _logger.Warn(
                $"Telegram bot for channel {_channel.Id} did not drain within 10s");
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
    }
}
