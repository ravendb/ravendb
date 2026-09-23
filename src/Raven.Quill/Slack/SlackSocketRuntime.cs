using Raven.Quill.Logging;
using System.Net.WebSockets;
using System.Threading.Channels;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Quill.Channels;
using Raven.Quill.Hosting;
using SlackNet;
using Channel = Raven.Quill.Channels.Channel;
using SlackNet.Events;
using SlackNet.SocketMode;

namespace Raven.Quill.Slack;

internal sealed class SlackSocketRuntime
{
    private const string HelloType = "hello";
    private const string DisconnectType = "disconnect";
    private const string EventsApiType = "events_api";

    private const string SocketModeDisabledError =
        "slack disabled Socket Mode for this app; turn it on under the app's Socket Mode page";

    private static readonly TimeSpan MinBackoff = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan StopTimeout = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan KeepAliveInterval = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan KeepAliveTimeout = TimeSpan.FromSeconds(20);

    private static readonly JsonSerializer Serializer = JsonSerializer.Create(SlackApiClient.JsonSettings.SerializerSettings);

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
        finally
        {
            _health.RecordSocketStopped(_database, _shortChannelId);
        }

        _cts.Dispose();
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
            TimeSpan? retryAfter = null;
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
                retryAfter = (e as SlackApiException)?.RetryAfter;
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

            var delay = reconnectRequested
                ? TimeSpan.FromMilliseconds(Random.Shared.Next(250, 1000))
                : retryAfter > _backoff ? retryAfter.Value : _backoff;

            try
            {
                await Task.Delay(delay, _cts.Token);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            if (reconnectRequested)
                continue;

            var doubled = _backoff * 2 + TimeSpan.FromMilliseconds(Random.Shared.Next(0, 250));
            _backoff = doubled < _options.SocketBackoffMax ? doubled : _options.SocketBackoffMax;
        }
    }

    private async Task<(string? Fatal, bool ReconnectRequested)> ConnectAndPumpAsync()
    {
        var url = await OpenSocketUrlAsync();

        var client = new ClientWebSocket();
        client.Options.KeepAliveInterval = KeepAliveInterval;
        client.Options.KeepAliveTimeout = KeepAliveTimeout;
        using var socket = new WebSocketWrapper(client, url);

        var frames = System.Threading.Channels.Channel.CreateUnbounded<string>(
            new UnboundedChannelOptions { SingleReader = true });
        using var subscription = socket.Messages.Subscribe(
            frame => frames.Writer.TryWrite(frame),
            _ => frames.Writer.TryComplete(),
            () => frames.Writer.TryComplete());
        _ = socket.Closed.ContinueWith(_ => frames.Writer.TryComplete(), TaskScheduler.Default);

        var hello = await HandshakeAsync(socket, frames.Reader) ??
                    throw new InvalidOperationException("slack closed the socket before sending a hello frame");

        if (TypeOf(hello) != HelloType)
            throw new InvalidOperationException(
                $"slack opened the socket with a '{TypeOf(hello) ?? "(untyped)"}' frame instead of hello");

        OnConnected();

        try
        {
            while (true)
            {
                var frame = await ReceiveFrameAsync(frames.Reader, _cts.Token);
                if (frame is null)
                    return (null, false);

                if (frame.Value<string>("envelope_id") is { Length: > 0 } envelopeId)
                    await AcknowledgeAsync(socket, envelopeId);

                switch (TypeOf(frame))
                {
                    case DisconnectType:
                        return OnDisconnectRequested(frame.Value<string>("reason"));

                    case EventsApiType:
                        OnEvent(frame);
                        break;
                }
            }
        }
        finally
        {
            _health.RecordSocketDisconnected(_database, _shortChannelId, null);
        }
    }

    private async Task<JObject?> HandshakeAsync(IWebSocket socket, ChannelReader<string> frames)
    {
        using var handshake = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
        handshake.CancelAfter(_options.SocketHandshakeTimeout);

        try
        {
            if (await socket.Open(handshake.Token) == false)
                throw new InvalidOperationException("slack refused the websocket handshake");

            return await ReceiveFrameAsync(frames, handshake.Token);
        }
        catch (OperationCanceledException) when (_cts.IsCancellationRequested == false)
        {
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

    private void OnEvent(JObject frame)
    {
        EventCallback? payload;
        try
        {
            payload = frame.ToObject<EventEnvelope>(Serializer)?.Payload;
        }
        catch (JsonException e)
        {
            if (_logger.IsDebugEnabled)
                _logger.Debug($"Dropped an unparseable Slack event for channel {_shortChannelId}: {e.Message}");
            return;
        }

        if (payload?.Event is not MessageEvent message)
            return;

        if (payload.TeamId != _settings.TeamId)
        {
            if (_logger.IsDebugEnabled)
                _logger.Debug($"Dropped a Slack event for channel {_shortChannelId} from foreign team {payload.TeamId}");
            return;
        }

        if (message.ChannelType != "im")
            return;

        if (string.IsNullOrEmpty(message.BotId) == false ||
            string.IsNullOrEmpty(message.User) || message.User == _settings.BotUserId)
            return;

        var kind = message switch
        {
            SlackNet.Events.FileShare => "unsupported",
            { Subtype: null or "" } => "text",
            _ => null,
        };
        if (kind is null || string.IsNullOrEmpty(message.Channel))
            return;

        _health.RecordInbound(_database, _shortChannelId);
        _processor.Enqueue(
            _database, _channelDocId, message.User, message.Channel, payload.EventId ?? "", kind, message.Text);
    }

    private async Task<JObject?> ReceiveFrameAsync(ChannelReader<string> frames, CancellationToken ct)
    {
        while (true)
        {
            string text;
            try
            {
                text = await frames.ReadAsync(ct);
            }
            catch (ChannelClosedException)
            {
                return null;
            }

            try
            {
                return JObject.Parse(text);
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

    private static Task AcknowledgeAsync(IWebSocket socket, string envelopeId)
    {
        if (socket.State != WebSocketState.Open)
            return Task.CompletedTask;

        var ack = new Acknowledgement { EnvelopeId = envelopeId };
        return socket.Send(JsonConvert.SerializeObject(ack, SlackApiClient.JsonSettings.SerializerSettings));
    }

    private static string? TypeOf(JObject frame) => frame.Value<string>("type");
}
