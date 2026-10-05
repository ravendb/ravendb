using Raven.Quill.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Quill.Channels;
using Raven.Quill.Hosting;
using SlackNet;
using SlackNet.SocketMode;
using Channel = Raven.Quill.Channels.Channel;

namespace Raven.Quill.Slack;

internal sealed class SlackSocketRuntime
{
    private const string SocketModeDisabledError =
        "slack disabled Socket Mode for this app; turn it on under the app's Socket Mode page";

    private const int PassesBeforeConnectionLost = 2;

    private static readonly TimeSpan StopTimeout = TimeSpan.FromSeconds(10);

    private readonly string _shortChannelId;
    private readonly string _botUserId;
    private readonly SlackOptions _options;
    private readonly QuillLogger<SlackChannelManager> _logger;
    private readonly ISlackSocketModeClient _client;
    private readonly IDisposable _frames;
    private readonly CancellationTokenSource _cts = new();
    private readonly object _exitLock = new();

    private Task _run = Task.CompletedTask;
    private volatile bool _connected;
    private volatile bool _canRestart = true;
    private int _disconnectedPasses;
    private TimeSpan _restartDelay;
    private long _exitedAtTicks;

    private SlackSocketRuntime(
        string database, Channel channel, string? channelChangeVector, SlackSdk sdk,
        SlackInboundProcessor processor, SlackOptions options,
        QuillLogger<SlackChannelManager> logger)
    {
        var settings = channel.Slack!;

        _shortChannelId = channel.ShortId;
        _botUserId = settings.BotUserId;
        _options = options;
        _logger = logger;
        ChannelChangeVector = channelChangeVector;

        var handler = new SlackMessageHandler(database, channel.Id!, settings, processor, Health);
        var socket = sdk.NewSocketClient(settings.AppToken, handler, new SlackNetLogger(channel.ShortId, logger));
        _client = socket.Client;
        _frames = socket.RawMessages.Subscribe(OnRawMessage);
    }

    public string? ChannelChangeVector { get; }

    public ChannelConnectionHealth Health { get; } = new();

    public bool CanRestart => _canRestart;

    public TimeSpan RestartDelay
    {
        get
        {
            lock (_exitLock)
                return _restartDelay;
        }
    }

    public DateTime? ExitedAt
    {
        get
        {
            var ticks = Interlocked.Read(ref _exitedAtTicks);
            return ticks == 0 ? null : new DateTime(ticks, DateTimeKind.Utc);
        }
    }

    public static SlackSocketRuntime Start(
        string database, Channel channel, string? channelChangeVector, SlackSdk sdk,
        SlackInboundProcessor processor, SlackOptions options,
        QuillLogger<SlackChannelManager> logger)
    {
        var runtime = new SlackSocketRuntime(
            database, channel, channelChangeVector, sdk, processor, options, logger);

        runtime._run = Task.Run(runtime.ConnectAsync);
        return runtime;
    }

    public void CheckConnection()
    {
        if (_connected == false || ExitedAt is not null)
            return;

        if (_client.Connected)
        {
            _disconnectedPasses = 0;
            return;
        }

        if (++_disconnectedPasses >= PassesBeforeConnectionLost)
            ReplaceQuietly();
    }

    public async Task StopAsync()
    {
        await _cts.CancelAsync();
        _frames.Dispose();

        try
        {
            await Task.WhenAll(_run, _client.DisposeAsync().AsTask()).WaitAsync(StopTimeout);
        }
        catch (TimeoutException)
        {
            if (_logger.IsWarnEnabled)
                _logger.Warn($"Slack socket for channel {_shortChannelId} did not stop within {StopTimeout}");
            return;
        }

        _cts.Dispose();
    }

    private async Task ConnectAsync()
    {
        try
        {
            await _client.Connect(new SocketModeConnectionOptions { NumberOfConnections = 1 }, _cts.Token);
            _connected = true;
            if (ExitedAt is null)
                Health.Connected();
            if (_logger.IsInfoEnabled)
                _logger.Info($"Slack socket connected for channel {_shortChannelId} (bot {_botUserId})");
        }
        catch (OperationCanceledException) when (_cts.IsCancellationRequested)
        {
        }
        catch (Exception e)
        {
            var error = SlackApiErrors.Translate(e, "apps.connections.open");
            var tokenError = SlackApiErrors.DescribeAppTokenError(error?.Error);
            _canRestart = tokenError is null || SlackApiErrors.AppTokenErrorIsFixableInSlack(error!.Error);
            Exit(tokenError ?? error?.Message ?? e.Message, _options.SocketRestartDelay);
        }
    }

    private void OnRawMessage(RawSocketMessage raw)
    {
        if (raw.Message.Contains("link_disabled", StringComparison.Ordinal) == false)
            return;

        JObject frame;
        try
        {
            frame = JObject.Parse(raw.Message);
        }
        catch (JsonException)
        {
            return;
        }

        if (frame.Value<string>("type") != "disconnect" || frame.Value<string>("reason") != "link_disabled")
            return;

        Exit(SocketModeDisabledError, _options.SocketRestartDelay);
        _ = Task.Run(_client.DisconnectAsync);
    }

    private void ReplaceQuietly()
    {
        if (MarkExited(TimeSpan.Zero) == false)
            return;

        Health.Disconnected(error: null);
        if (_logger.IsInfoEnabled)
            _logger.Info($"Slack socket for channel {_shortChannelId} stayed down; replacing it");
    }

    private void Exit(string error, TimeSpan restartDelay)
    {
        if (MarkExited(restartDelay) == false)
            return;

        Health.Disconnected(error);
        if (_logger.IsWarnEnabled)
            _logger.Warn($"Slack socket for channel {_shortChannelId} exited: {error}");
    }

    private bool MarkExited(TimeSpan restartDelay)
    {
        lock (_exitLock)
        {
            if (_exitedAtTicks != 0)
                return false;

            _restartDelay = restartDelay;
            Interlocked.Exchange(ref _exitedAtTicks, DateTime.UtcNow.Ticks);
            return true;
        }
    }
}
