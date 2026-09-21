using Raven.Quill.Logging;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using Raven.Quill.Channels;
using Raven.Quill.Hosting;

namespace Raven.Quill.Slack;

internal sealed class SlackSocketRuntime
{
    private const string SocketModeDisabledError =
        "slack disabled Socket Mode for this app; turn it on under the app's Socket Mode page";

    private static readonly TimeSpan MinBackoff = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan StopTimeout = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan KeepAliveInterval = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan KeepAliveTimeout = TimeSpan.FromSeconds(20);

    private static readonly JsonSerializerOptions JsonOptions = new();

    private readonly string _database;
    private readonly string _shortChannelId;
    private readonly string _channelDocId;
    private readonly SlackSettings _settings;
    private readonly SlackOptions _options;
    private readonly SlackInboundProcessor _processor;
    private readonly SlackHealthRegistry _health;
    private readonly IServiceScopeFactory _scopes;
    private readonly QuillLogger<SlackChannelManager> _logger;

    private readonly CancellationTokenSource _cts = new();
    private readonly SemaphoreSlim _sendLock = new(1, 1);
    private readonly byte[] _receiveBuffer = new byte[8 * 1024];
    private readonly MemoryStream _frameBuffer = new();

    private volatile bool _canRestart = true;
    private Task _run = Task.CompletedTask;
    private TimeSpan _backoff = MinBackoff;
    private long _exitedAtTicks;

    private SlackSocketRuntime(
        string database, Channel channel, string? channelChangeVector, SlackSettings settings,
        SlackInboundProcessor processor, SlackHealthRegistry health, IServiceScopeFactory scopes,
        SlackOptions options, QuillLogger<SlackChannelManager> logger)
    {
        _database = database;
        _shortChannelId = channel.ShortId;
        _channelDocId = channel.Id!;
        _settings = settings;
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

    public static SlackSocketRuntime Start(
        string database, Channel channel, string? channelChangeVector, SlackInboundProcessor processor,
        SlackHealthRegistry health, IServiceScopeFactory scopes, SlackOptions options,
        QuillLogger<SlackChannelManager> logger)
    {
        var runtime = new SlackSocketRuntime(
            database, channel, channelChangeVector, channel.Slack!, processor, health, scopes, options, logger);

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
                _logger.Warn($"Slack socket for channel {_shortChannelId} did not stop within {StopTimeout}");
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
            var reconnectRequested = false;
            try
            {
                (fatal, reconnectRequested) = await ConnectAndPumpAsync();
            }
            catch (OperationCanceledException) when (_cts.IsCancellationRequested)
            {
                return;
            }
            catch (SlackApiException e) when (e.RejectsAppToken)
            {
                _canRestart = e.Error is "missing_scope" or "not_allowed_token_type";
                fatal = e.Error switch
                {
                    "not_allowed_token_type" =>
                        "slack refused the app-level token because it is the wrong token type; paste the xapp- token from the app's Basic Information page",
                    "missing_scope" =>
                        "the app-level token lacks the connections:write scope; regenerate it with that scope on the app's Basic Information page",
                    _ => "slack rejected the app-level token; regenerate it on the app's Basic Information page and rotate it on this channel",
                };
            }
            catch (Exception e)
            {
                fatal = null;
                _health.RecordSocketDisconnected(_database, _shortChannelId, e.Message);
                if (_logger.IsWarnEnabled)
                    _logger.Warn($"Slack socket attempt failed for channel {_shortChannelId}: {e.Message}");
            }

            if (fatal is not null)
            {
                _health.RecordSocketDisconnected(_database, _shortChannelId, fatal);
                if (_logger.IsErrorEnabled)
                    _logger.Error($"Slack socket stopped for channel {_shortChannelId}: {fatal}");
                return;
            }

            if (reconnectRequested)
                continue;

            try
            {
                await Task.Delay(_backoff, _cts.Token);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            var doubled = _backoff * 2 + TimeSpan.FromMilliseconds(Random.Shared.Next(0, 250));
            _backoff = doubled < _options.SocketBackoffMax ? doubled : _options.SocketBackoffMax;
        }
    }

    private async Task<(string? Fatal, bool ReconnectRequested)> ConnectAndPumpAsync()
    {
        var url = await OpenSocketUrlAsync();

        using var socket = new ClientWebSocket();
        socket.Options.KeepAliveInterval = KeepAliveInterval;
        socket.Options.KeepAliveTimeout = KeepAliveTimeout;
        _frameBuffer.SetLength(0);

        var hello = await HandshakeAsync(socket, url) ??
                    throw new InvalidOperationException("slack closed the socket before sending a hello frame");

        if (hello.Type != SlackSocketFrame.HelloType)
            throw new InvalidOperationException(
                $"slack opened the socket with a '{hello.Type ?? "(untyped)"}' frame instead of hello");

        OnConnected();

        try
        {
            while (true)
            {
                var frame = await ReceiveFrameAsync(socket, _cts.Token);
                if (frame is null)
                    return (null, false);

                if (frame.EnvelopeId is { Length: > 0 } envelopeId)
                    await SendAsync(socket, new { envelope_id = envelopeId }, _cts.Token);

                switch (frame.Type)
                {
                    case SlackSocketFrame.DisconnectType:
                        return OnDisconnectRequested(frame.Reason);

                    case SlackSocketFrame.EventsApiType:
                        OnEvent(frame.Payload);
                        break;
                }
            }
        }
        finally
        {
            _health.RecordSocketDisconnected(_database, _shortChannelId, null);
        }
    }

    private async Task<SlackSocketFrame?> HandshakeAsync(ClientWebSocket socket, string url)
    {
        using var handshake = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
        handshake.CancelAfter(_options.SocketHandshakeTimeout);

        try
        {
            await socket.ConnectAsync(new Uri(url), handshake.Token);
            return await ReceiveFrameAsync(socket, handshake.Token);
        }
        catch (OperationCanceledException) when (_cts.IsCancellationRequested == false)
        {
            socket.Abort();
            throw new TimeoutException(
                $"slack did not send a hello frame within {_options.SocketHandshakeTimeout}");
        }
    }

    private void OnConnected()
    {
        _backoff = MinBackoff;
        _health.RecordSocketConnected(_database, _shortChannelId);
        if (_logger.IsInfoEnabled)
            _logger.Info($"Slack socket connected for channel {_shortChannelId} (bot {_settings.BotUserId})");
    }

    private (string? Fatal, bool ReconnectRequested) OnDisconnectRequested(string? reason)
    {
        if (reason == "link_disabled")
            return (SocketModeDisabledError, false);

        if (_logger.IsInfoEnabled)
            _logger.Info($"Slack asked channel {_shortChannelId} to reconnect its socket ({reason ?? "no reason"})");
        return (null, true);
    }

    private void OnEvent(JsonElement? payloadElement)
    {
        SlackEventPayload? payload;
        try
        {
            payload = payloadElement?.Deserialize<SlackEventPayload>(JsonOptions);
        }
        catch (JsonException e)
        {
            if (_logger.IsDebugEnabled)
                _logger.Debug($"Dropped an unparseable Slack event for channel {_shortChannelId}: {e.Message}");
            return;
        }

        if (payload?.Type != "event_callback" || payload.Event is not { } message)
            return;

        if (payload.TeamId != _settings.TeamId)
        {
            if (_logger.IsDebugEnabled)
                _logger.Debug($"Dropped a Slack event for channel {_shortChannelId} from foreign team {payload.TeamId}");
            return;
        }

        if (message.Type != "message" || message.ChannelType != "im")
            return;

        if (string.IsNullOrEmpty(message.BotId) == false ||
            string.IsNullOrEmpty(message.User) || message.User == _settings.BotUserId)
            return;

        var kind = message.Subtype switch
        {
            null or "" => "text",
            "file_share" => "unsupported",
            _ => null,
        };
        if (kind is null || string.IsNullOrEmpty(message.Channel))
            return;

        _health.RecordInbound(_database, _shortChannelId);
        _processor.Enqueue(
            _database, _channelDocId, message.User, message.Channel, payload.EventId ?? "", kind, message.Text);
    }

    private async Task<SlackSocketFrame?> ReceiveFrameAsync(ClientWebSocket socket, CancellationToken ct)
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

            if (_frameBuffer.Length + result.Count > _options.MaxSocketFrameBytes)
                throw new InvalidOperationException(
                    $"slack sent a socket frame over {_options.MaxSocketFrameBytes} bytes");

            _frameBuffer.Write(_receiveBuffer, 0, result.Count);

            if (result.EndOfMessage == false)
                continue;

            var text = Encoding.UTF8.GetString(_frameBuffer.GetBuffer(), 0, (int)_frameBuffer.Length);
            _frameBuffer.SetLength(0);

            try
            {
                return JsonSerializer.Deserialize<SlackSocketFrame>(text, JsonOptions);
            }
            catch (JsonException e)
            {
                if (_logger.IsDebugEnabled)
                    _logger.Debug($"Dropped an unparseable Slack socket frame for channel {_shortChannelId}: {e.Message}");
            }
        }
    }

    private async Task<string> OpenSocketUrlAsync()
    {
        await using var scope = _scopes.CreateAsyncScope();
        var slack = scope.ServiceProvider.GetRequiredService<ISlackClient>();
        return await slack.OpenSocketAsync(_settings.AppToken, _cts.Token);
    }

    private async Task SendAsync(ClientWebSocket socket, object payload, CancellationToken ct)
    {
        var bytes = JsonSerializer.SerializeToUtf8Bytes(payload, JsonOptions);

        await _sendLock.WaitAsync(ct);
        try
        {
            if (socket.State == WebSocketState.Open)
                await socket.SendAsync(bytes, WebSocketMessageType.Text, true, ct);
        }
        finally
        {
            _sendLock.Release();
        }
    }
}
