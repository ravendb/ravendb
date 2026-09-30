using Raven.Quill.Logging;
using System.Net;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using Raven.Quill.Channels;
using Raven.Quill.Hosting;

namespace Raven.Quill.Discord;

internal sealed class DiscordGatewayRuntime
{
    private const int DirectMessagesIntent = 1 << 12;
    private const int DefaultMessageType = 0;
    private const int ReplyMessageType = 19;
    private const int AttemptsBeforeSessionReset = 3;

    private static readonly TimeSpan MinBackoff = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan StopTimeout = TimeSpan.FromSeconds(10);

    private static readonly JsonSerializerOptions JsonOptions = new();

    private readonly string _database;
    private readonly string _shortChannelId;
    private readonly string _channelDocId;
    private readonly string _botToken;
    private readonly string _botUserId;
    private readonly DiscordOptions _options;
    private readonly DiscordInboundProcessor _processor;
    private readonly DiscordHealthRegistry _health;
    private readonly IServiceScopeFactory _scopes;
    private readonly QuillLogger<DiscordChannelManager> _logger;

    private readonly CancellationTokenSource _cts = new();
    private readonly SemaphoreSlim _sendLock = new(1, 1);
    private readonly byte[] _receiveBuffer = new byte[8 * 1024];
    private readonly MemoryStream _frameBuffer = new();

    private volatile bool _canRestart = true;
    private Task _run = Task.CompletedTask;
    private TimeSpan _backoff = MinBackoff;
    private long _seq = -1;
    private long _exitedAtTicks;
    private int _awaitingAck;
    private int _attemptsSinceConnected;
    private string? _sessionId;
    private string? _resumeUrl;

    private DiscordGatewayRuntime(
        string database, Channel channel, string? channelChangeVector, DiscordSettings settings,
        DiscordInboundProcessor processor, DiscordHealthRegistry health, IServiceScopeFactory scopes,
        DiscordOptions options, QuillLogger<DiscordChannelManager> logger)
    {
        _database = database;
        _shortChannelId = channel.ShortId;
        _channelDocId = channel.Id!;
        _botToken = settings.BotToken;
        _botUserId = settings.BotUserId;
        _processor = processor;
        _health = health;
        _scopes = scopes;
        _options = options;
        _logger = logger;
        ChannelChangeVector = channelChangeVector;
    }

    public string? ChannelChangeVector { get; }

    public bool CanRestart => _canRestart;

    public DateTime? ExitedAt
    {
        get
        {
            var ticks = Interlocked.Read(ref _exitedAtTicks);
            return ticks == 0 ? null : new DateTime(ticks, DateTimeKind.Utc);
        }
    }

    public static DiscordGatewayRuntime Start(
        string database, Channel channel, string? channelChangeVector, DiscordInboundProcessor processor,
        DiscordHealthRegistry health, IServiceScopeFactory scopes, DiscordOptions options, QuillLogger<DiscordChannelManager> logger)
    {
        var runtime = new DiscordGatewayRuntime(
            database, channel, channelChangeVector, channel.Discord!, processor, health, scopes, options, logger);

        runtime._run = Task.Run(runtime.RunAsync);
        return runtime;
    }

    public async Task StopAsync()
    {
        await _cts.CancelAsync();

        try
        {
            await _run.WaitAsync(StopTimeout);
        }
        catch (TimeoutException)
        {
            if (_logger.IsWarnEnabled)
                _logger.Warn($"Discord gateway for channel {_shortChannelId} did not stop within {StopTimeout}");
            return;
        }

        _cts.Dispose();
        _sendLock.Dispose();
        _frameBuffer.Dispose();
    }

    private async Task RunAsync()
    {
        try
        {
            await ReconnectLoopAsync();
        }
        finally
        {
            Interlocked.Exchange(ref _exitedAtTicks, DateTime.UtcNow.Ticks);
        }
    }

    private async Task ReconnectLoopAsync()
    {
        while (_cts.IsCancellationRequested == false)
        {
            string? fatal;
            try
            {
                fatal = await ConnectAndPumpAsync();
            }
            catch (OperationCanceledException) when (_cts.IsCancellationRequested)
            {
                return;
            }
            catch (DiscordApiException e) when (e.Status is HttpStatusCode.Unauthorized or HttpStatusCode.Forbidden)
            {
                fatal = "discord rejected the bot token; reset it on the app's Bot page and reconnect the channel";
            }
            catch (Exception e)
            {
                fatal = null;
                _health.RecordGatewayDisconnected(_database, _shortChannelId, e.Message);
                if (_logger.IsWarnEnabled)
                    _logger.Warn($"Discord gateway attempt failed for channel {_shortChannelId}: {e.Message}");
            }

            if (fatal is not null)
            {
                _health.RecordGatewayDisconnected(_database, _shortChannelId, fatal);
                if (_logger.IsErrorEnabled)
                    _logger.Error($"Discord gateway stopped for channel {_shortChannelId}: {fatal}");
                return;
            }

            if (++_attemptsSinceConnected >= AttemptsBeforeSessionReset && _sessionId is not null)
            {
                if (_logger.IsWarnEnabled)
                    _logger.Warn($"Discord gateway for channel {_shortChannelId} dropped its cached session after {_attemptsSinceConnected} " +
                        "attempts that never connected");
                ForgetSession();
            }

            try
            {
                await Task.Delay(_backoff, _cts.Token);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            var doubled = _backoff * 2 + TimeSpan.FromMilliseconds(Random.Shared.Next(0, 250));
            _backoff = doubled < _options.GatewayBackoffMax ? doubled : _options.GatewayBackoffMax;
        }
    }

    private async Task<string?> ConnectAndPumpAsync()
    {
        var resuming = _sessionId is not null && _resumeUrl is not null;
        var url = resuming ? _resumeUrl! : await DiscoverGatewayUrlAsync();

        using var socket = new ClientWebSocket();
        _frameBuffer.SetLength(0);

        var hello = await HandshakeAsync(socket, url);

        using var connection = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
        var ct = connection.Token;

        if (hello is null)
            return OnClosed(socket.CloseStatus);

        if (hello.Op != DiscordGatewayOpcode.Hello)
            return null;

        var interval = TimeSpan.FromMilliseconds(
            hello.D?.Deserialize<DiscordHelloPayload>(JsonOptions)?.HeartbeatInterval ?? 0);
        if (interval <= TimeSpan.Zero)
            return null;

        await SendAsync(socket, resuming ? ResumePayload() : IdentifyPayload(), ct);

        Volatile.Write(ref _awaitingAck, 0);
        var heartbeat = HeartbeatLoopAsync(socket, interval, ct);

        try
        {
            while (true)
            {
                var frame = await ReceiveFrameAsync(socket, ct);
                if (frame is null)
                    return OnClosed(socket.CloseStatus);

                switch (frame.Op)
                {
                    case DiscordGatewayOpcode.HeartbeatAck:
                        Volatile.Write(ref _awaitingAck, 0);
                        break;

                    case DiscordGatewayOpcode.Heartbeat:
                        await SendHeartbeatAsync(socket, ct);
                        break;

                    case DiscordGatewayOpcode.Reconnect:
                        return null;

                    case DiscordGatewayOpcode.InvalidSession:
                        if (frame.D?.ValueKind != JsonValueKind.True)
                            ForgetSession();
                        return null;

                    case DiscordGatewayOpcode.Dispatch:
                        if (frame.S is { } seq)
                            Interlocked.Exchange(ref _seq, seq);
                        OnDispatch(frame);
                        break;
                }
            }
        }
        finally
        {
            await connection.CancelAsync();
            try
            {
                await heartbeat;
            }
            catch (Exception)
            {
            }

            _health.RecordGatewayDisconnected(_database, _shortChannelId, null);
        }
    }

    private async Task<DiscordGatewayFrame?> HandshakeAsync(ClientWebSocket socket, string url)
    {
        using var handshake = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
        handshake.CancelAfter(_options.GatewayHandshakeTimeout);

        try
        {
            await socket.ConnectAsync(GatewayUri(url), handshake.Token);
            return await ReceiveFrameAsync(socket, handshake.Token);
        }
        catch (OperationCanceledException) when (_cts.IsCancellationRequested == false)
        {
            socket.Abort();
            throw new TimeoutException(
                $"discord did not send a hello frame within {_options.GatewayHandshakeTimeout}");
        }
    }

    private void OnDispatch(DiscordGatewayFrame frame)
    {
        switch (frame.T)
        {
            case "READY":
                var ready = frame.D?.Deserialize<DiscordReadyPayload>(JsonOptions);
                _sessionId = ready?.SessionId;
                _resumeUrl = ready?.ResumeGatewayUrl;
                OnConnected();
                break;

            case "RESUMED":
                OnConnected();
                break;

            case "MESSAGE_CREATE":
                OnMessage(frame.D);
                break;
        }
    }

    private void OnConnected()
    {
        _backoff = MinBackoff;
        _attemptsSinceConnected = 0;
        _health.RecordGatewayConnected(_database, _shortChannelId);
        if (_logger.IsInfoEnabled)
            _logger.Info($"Discord gateway connected for channel {_shortChannelId} (bot {_botUserId})");
    }

    private void OnMessage(JsonElement? data)
    {
        var message = data?.Deserialize<DiscordMessagePayload>(JsonOptions);
        if (message is null)
            return;

        if (string.IsNullOrEmpty(message.GuildId) == false)
            return;

        if (message.Author is not { Id.Length: > 0 } author || author.Bot == true || author.Id == _botUserId)
            return;

        if (string.IsNullOrEmpty(message.ChannelId))
            return;

        if (message.Type is not (DefaultMessageType or ReplyMessageType))
            return;

        var content = message.Content ?? "";
        var kind = message.Attachments is { Length: > 0 } || content.Trim().Length == 0
            ? "unsupported"
            : "text";

        _health.RecordInbound(_database, _shortChannelId);
        _processor.Enqueue(
            _database, _channelDocId, author.Id, author.Username, message.ChannelId,
            message.Id ?? "", kind, content);
    }

    private async Task HeartbeatLoopAsync(ClientWebSocket socket, TimeSpan interval, CancellationToken ct)
    {
        try
        {
            await Task.Delay(interval * Random.Shared.NextDouble(), ct);

            while (ct.IsCancellationRequested == false)
            {
                if (Interlocked.Exchange(ref _awaitingAck, 1) == 1)
                {
                    if (_logger.IsWarnEnabled)
                        _logger.Warn($"Discord gateway heartbeat went unacknowledged for channel {_shortChannelId}; reconnecting");
                    socket.Abort();
                    return;
                }

                await SendHeartbeatAsync(socket, ct);
                await Task.Delay(interval, ct);
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception e)
        {
            if (_logger.IsDebugEnabled)
                _logger.Debug($"Discord gateway heartbeat stopped for channel {_shortChannelId}: {e.Message}");
            socket.Abort();
        }
    }

    private async Task<DiscordGatewayFrame?> ReceiveFrameAsync(ClientWebSocket socket, CancellationToken ct)
    {
        while (true)
        {
            WebSocketReceiveResult result;
            try
            {
                result = await socket.ReceiveAsync(_receiveBuffer, ct);
            }
            catch (WebSocketException)
            {
                return null;
            }

            if (result.MessageType == WebSocketMessageType.Close)
                return null;

            if (_frameBuffer.Length + result.Count > _options.MaxGatewayFrameBytes)
                throw new InvalidOperationException(
                    $"discord sent a gateway frame over {_options.MaxGatewayFrameBytes} bytes");

            _frameBuffer.Write(_receiveBuffer, 0, result.Count);

            if (result.EndOfMessage == false)
                continue;

            var text = Encoding.UTF8.GetString(_frameBuffer.GetBuffer(), 0, (int)_frameBuffer.Length);
            _frameBuffer.SetLength(0);

            try
            {
                return JsonSerializer.Deserialize<DiscordGatewayFrame>(text, JsonOptions);
            }
            catch (JsonException e)
            {
                if (_logger.IsDebugEnabled)
                    _logger.Debug($"Dropped an unparseable Discord gateway frame for channel {_shortChannelId}: {e.Message}");
            }
        }
    }

    private async Task<string> DiscoverGatewayUrlAsync()
    {
        await using var scope = _scopes.CreateAsyncScope();
        var discord = scope.ServiceProvider.GetRequiredService<IDiscordClient>();
        return await discord.GetGatewayUrlAsync(_botToken, _cts.Token);
    }

    private Task SendHeartbeatAsync(ClientWebSocket socket, CancellationToken ct)
    {
        var seq = Interlocked.Read(ref _seq);
        return SendAsync(socket, new { op = DiscordGatewayOpcode.Heartbeat, d = seq < 0 ? null : (long?)seq }, ct);
    }

    private async Task SendAsync(ClientWebSocket socket, object payload, CancellationToken ct)
    {
        var bytes = JsonSerializer.SerializeToUtf8Bytes(payload, JsonOptions);

        await _sendLock.WaitAsync(ct);
        try
        {
            await socket.SendAsync(bytes, WebSocketMessageType.Text, true, ct);
        }
        finally
        {
            _sendLock.Release();
        }
    }

    private object IdentifyPayload() => new
    {
        op = DiscordGatewayOpcode.Identify,
        d = new
        {
            token = _botToken,
            intents = DirectMessagesIntent,
            properties = new { os = "linux", browser = "raven-quill", device = "raven-quill" },
        },
    };

    private object ResumePayload() => new
    {
        op = DiscordGatewayOpcode.Resume,
        d = new { token = _botToken, session_id = _sessionId, seq = Interlocked.Read(ref _seq) },
    };

    private string? OnClosed(WebSocketCloseStatus? status)
    {
        if ((int?)status is 4007 or 4009)
            ForgetSession();

        var fatal = FatalReasonFor(status);
        if (fatal is not null)
            _canRestart = (int?)status is 4013 or 4014;

        return fatal;
    }

    private void ForgetSession()
    {
        _sessionId = null;
        _resumeUrl = null;
        Interlocked.Exchange(ref _seq, -1);
    }

    private static Uri GatewayUri(string url)
    {
        var separator = url.Contains('?') ? '&' : '?';
        return new Uri($"{url}{separator}v=10&encoding=json");
    }

    private static string? FatalReasonFor(WebSocketCloseStatus? status) => (int?)status switch
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
