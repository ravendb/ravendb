using System.Globalization;
using System.Net;
using Discord;
using Discord.Net;
using Discord.WebSocket;
using Raven.Quill.Channels;
using Raven.Quill.Hosting;
using Raven.Quill.Logging;

namespace Raven.Quill.Discord;

internal sealed class DiscordRuntime : IChannelRuntime
{
    private const string SdkReadyFailure = "Processing READY failed";

    private static readonly TimeSpan MinBackoff = TimeSpan.FromSeconds(1);

    private readonly Channel _channel;
    private readonly string _botToken;
    private readonly string _botUserId;
    private readonly DiscordOptions _options;
    private readonly ChannelChats<DiscordMessage> _chats;
    private readonly DiscordBotClient _bot;
    private readonly DiscordSdk _sdk;
    private readonly QuillLogger<DiscordRuntime> _logger;

    private readonly CancellationTokenSource _cts = new();

    private volatile bool _canRestart = true;
    private volatile string? _status;
    private volatile DiscordSocketClient? _client;
    private Task _run = Task.CompletedTask;
    private Task? _stop;
    private TimeSpan _backoff = MinBackoff;
    private long _exitedAtTicks;

    private DiscordRuntime(
        string database, Channel channel, string channelFingerprint,
        DiscordTurns turns, DiscordBotClient bot, ChannelChatsFactory chats, DiscordSdk sdk,
        DiscordOptions options, QuillLogger<DiscordRuntime> logger, ChannelConnectionHealth? previous)
    {
        var settings = channel.Discord!;

        Health = previous is null ? new ChannelConnectionHealth() : ChannelConnectionHealth.Continuing(previous);
        _channel = channel;
        _bot = bot;
        _botToken = settings.BotToken;
        _botUserId = settings.BotUserId;
        _chats = chats.Create(database, channel, turns, Health, logger.RavenLogger);
        _sdk = sdk;
        _options = options;
        _logger = logger;
        _status = channel.DisplayName;
        ChannelFingerprint = channelFingerprint;
    }

    public string ChannelFingerprint { get; }

    public ChannelConnectionHealth Health { get; }

    public bool IsRestartDue(DateTime now) =>
        _canRestart && ExitedAt is { } exitedAt && now - exitedAt >= _options.GatewayRestartDelay;

    public DateTime? ExitedAt
    {
        get
        {
            var ticks = Interlocked.Read(ref _exitedAtTicks);
            return ticks == 0 ? null : new DateTime(ticks, DateTimeKind.Utc);
        }
    }

    public static DiscordRuntime Start(
        string database, Channel channel, string channelFingerprint, DiscordTurns turns, DiscordBotClient bot,
        ChannelChatsFactory chats, DiscordSdk sdk, DiscordOptions options,
        QuillLogger<DiscordRuntime> logger, ChannelConnectionHealth? previous = null)
    {
        var runtime = new DiscordRuntime(
            database, channel, channelFingerprint, turns, bot, chats, sdk, options, logger, previous);

        runtime._run = Task.Run(runtime.RunAsync);
        return runtime;
    }

    public Task StopAsync() => _stop ??= StopCoreAsync();

    private async Task StopCoreAsync()
    {
        await _cts.CancelAsync();
        await Task.WhenAll(_run, _chats.StopAsync())
            .ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        await _bot.DisposeAsync();
        _cts.Dispose();
    }

    public void Apply(Channel channel)
    {
        if (channel.DisplayName == _status)
            return;

        _status = channel.DisplayName;
        if (_client is { } client && Health.IsConnected)
            _ = ShowStatusAsync(client);
    }

    private async Task RunAsync()
    {
        try
        {
            await _bot.LoginAsync(_cts.Token);
            await SessionLoopAsync();
        }
        catch (OperationCanceledException) when (_cts.IsCancellationRequested)
        {
        }
        catch (Exception e)
        {
            Health.MarkDisconnected(e.Message);
            if (_logger.IsErrorEnabled)
                _logger.Error(e, $"Discord gateway crashed for channel {_channel.ShortId}");
        }
        finally
        {
            Interlocked.Exchange(ref _exitedAtTicks, DateTime.UtcNow.Ticks);
        }
    }

    private async Task SessionLoopAsync()
    {
        while (true)
        {
            var fatal = await RunClientAsync();
            if (fatal is not null)
            {
                Health.MarkDisconnected(fatal);
                if (_logger.IsErrorEnabled)
                    _logger.Error($"Discord gateway stopped for channel {_channel.ShortId}: {fatal}");
                return;
            }

            if (_logger.IsWarnEnabled)
                _logger.Warn($"Discord gateway for channel {_channel.ShortId} dropped its session and starts a new one in {_backoff}");

            await Task.Delay(_backoff, _cts.Token);

            var doubled = _backoff * 2 + TimeSpan.FromMilliseconds(Random.Shared.Next(0, 250));
            _backoff = doubled < _options.GatewayBackoffMax ? doubled : _options.GatewayBackoffMax;
        }
    }

    private async Task<string?> RunClientAsync()
    {
        var exit = new TaskCompletionSource<string?>(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var client = _sdk.NewSocketClient();
        _client = client;

        client.Log += message =>
        {
            Log(message);
            return Task.CompletedTask;
        };

        client.Connected += async () =>
        {
            OnConnected();
            await ShowStatusAsync(client);
        };

        client.Disconnected += error =>
        {
            if (_cts.IsCancellationRequested || exit.Task.IsCompleted)
                return Task.CompletedTask;

            var fatal = FatalReasonFor(error);
            if (fatal is not null)
            {
                exit.TrySetResult(fatal);
                return Task.CompletedTask;
            }

            var reason = TransientReasonFor(error);
            Health.MarkDisconnected(reason);
            if (reason is not null && _logger.IsWarnEnabled)
                _logger.Warn($"Discord gateway attempt failed for channel {_channel.ShortId}: {reason}");

            if (NeedsNewClient(error))
                exit.TrySetResult(null);

            return Task.CompletedTask;
        };

        client.MessageReceived += message =>
        {
            OnMessage(message);
            return Task.CompletedTask;
        };

        try
        {
            await client.LoginAsync(TokenType.Bot, _botToken, validateToken: false);
            await client.StartAsync();

            return await exit.Task.WaitAsync(_cts.Token);
        }
        finally
        {
            _client = null;
            await client.StopAsync();
            Health.MarkDisconnected(null);
        }
    }

    private void OnConnected()
    {
        _backoff = MinBackoff;
        Health.MarkConnected();
        if (_logger.IsInfoEnabled)
            _logger.Info($"Discord gateway connected for channel {_channel.ShortId} (bot {_botUserId})");
    }

    private async Task ShowStatusAsync(DiscordSocketClient client)
    {
        var status = _status;
        if (string.IsNullOrWhiteSpace(status))
            return;

        try
        {
            await client.SetCustomStatusAsync(status);
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            if (_logger.IsDebugEnabled)
                _logger.Debug($"Discord custom status failed for channel {_channel.ShortId}: {e.Message}");
        }
    }

    private void OnMessage(SocketMessage message)
    {
        if (message.Channel is not IDMChannel dm)
            return;

        var author = message.Author;
        if (author is null || author.IsBot || Snowflake(author.Id) == _botUserId)
            return;

        if (message.Type is not (MessageType.Default or MessageType.Reply))
            return;

        var content = message.Content ?? "";
        var kind = message.Attachments.Count > 0 || content.Trim().Length == 0
            ? ChannelMessageKind.Unsupported
            : ChannelMessageKind.Prompt;

        Health.MarkReceived();
        _chats.Enqueue(new DiscordMessage(Snowflake(author.Id), author.Username, Snowflake(dm.Id), kind, content));
    }

    private void Log(LogMessage message)
    {
        var text = $"Discord.Net {message.Source} for channel {_channel.ShortId}: {message.Message}";

        if (message.Severity is LogSeverity.Critical or LogSeverity.Error)
        {
            if (_logger.IsWarnEnabled == false)
                return;

            if (message.Exception is null)
                _logger.Warn(text);
            else
                _logger.Warn(message.Exception, text);
            return;
        }

        if (_logger.IsDebugEnabled == false)
            return;

        if (message.Exception is null)
            _logger.Debug(text);
        else
            _logger.Debug(message.Exception, text);
    }

    private string? FatalReasonFor(Exception error)
    {
        if (CloseOf(error) is { } closed)
        {
            var fatal = FatalReasonFor(closed.CloseCode);
            if (fatal is not null)
                _canRestart = closed.CloseCode is 4013 or 4014;
            return fatal;
        }

        if (error is HttpException { HttpCode: HttpStatusCode.Unauthorized or HttpStatusCode.Forbidden })
        {
            _canRestart = true;
            return "discord rejected the bot token; reset it on the app's Bot page and reconnect the channel";
        }

        return null;
    }

    private string? TransientReasonFor(Exception error) => error switch
    {
        GatewayReconnectException or OperationCanceledException => null,
        RateLimitedException => error.Message,
        TimeoutException => $"discord gateway did not become ready within {_options.GatewayHandshakeTimeout}",
        _ when CloseOf(error) is not null => null,
        _ => error.Message,
    };

    private static bool NeedsNewClient(Exception error) =>
        CloseOf(error)?.CloseCode is 4006 or 4007 or 4009 || error.Message == SdkReadyFailure;

    private static WebSocketClosedException? CloseOf(Exception error) =>
        error as WebSocketClosedException ?? error.InnerException as WebSocketClosedException;

    private static string Snowflake(ulong id) => id.ToString(CultureInfo.InvariantCulture);

    private static string? FatalReasonFor(int closeCode) => closeCode switch
    {
        4004 => "discord rejected the bot token; reset it on the app's Bot page and reconnect the channel",
        4010 => "discord rejected the shard this appliance identified with",
        4011 => "this bot is in too many servers for a single gateway connection",
        4012 => "discord no longer supports the gateway version this appliance uses",
        4013 => "discord rejected the requested gateway intents",
        4014 => "discord rejected the direct messages intent for this app",
        _ => null,
    };
}
