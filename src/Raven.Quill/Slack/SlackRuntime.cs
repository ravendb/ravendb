using Raven.Quill.Logging;
using Raven.Quill.Channels;
using Raven.Quill.Hosting;
using SlackNet;
using SlackNet.SocketMode;
using Channel = Raven.Quill.Channels.Channel;

namespace Raven.Quill.Slack;

internal sealed class SlackRuntime : IChannelRuntime, SlackSdk.ISocketObserver
{
    private static readonly TimeSpan MinReconnectDelay = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan MaxReconnectDelay = TimeSpan.FromSeconds(30);

    private readonly string _shortChannelId;
    private readonly string _botUserId;
    private readonly string _appToken;
    private readonly SlackSdk _sdk;
    private readonly SlackOptions _options;
    private readonly QuillLogger<SlackRuntime> _logger;
    private readonly SlackMessageHandler _handler;
    private readonly SlackNetLogger _netLogger;
    private readonly ChannelChats<SlackMessage> _chats;
    private readonly CancellationTokenSource _cts = new();

    private volatile ISlackSocketModeClient _client;
    private Task _run = Task.CompletedTask;
    private volatile Task _replace = Task.CompletedTask;
    private volatile bool _canRestart = true;
    private long _restartDueTicks;
    private int _replacing;
    private readonly Lazy<Task> _stop;

    private SlackRuntime(
        string database, Channel channel, string channelFingerprint, SlackSdk sdk, SlackTurns turns,
        ChannelChatsFactory chats, SlackOptions options,
        QuillLogger<SlackRuntime> logger, ChannelConnectionHealth? previous)
    {
        var settings = channel.Slack!;

        Health = previous is null ? new ChannelConnectionHealth() : ChannelConnectionHealth.Continuing(previous);
        _shortChannelId = channel.ShortId;
        _botUserId = settings.BotUserId;
        _appToken = settings.AppToken;
        _sdk = sdk;
        _options = options;
        _logger = logger;
        ChannelFingerprint = channelFingerprint;
        _chats = chats.Create(database, channel, turns, Health, logger.RavenLogger);

        _handler = new SlackMessageHandler(channel, _chats, Health);
        _netLogger = new SlackNetLogger(this, channel.ShortId, logger);
        _client = sdk.NewSocketClient(_appToken, _handler, _netLogger, this);
        _stop = new Lazy<Task>(StopCoreAsync);
    }

    public string ChannelFingerprint { get; }

    public ChannelConnectionHealth Health { get; }

    public bool IsConnected => Health.IsConnected;

    public bool IsRestartDue(DateTime now)
    {
        var due = Interlocked.Read(ref _restartDueTicks);
        return _canRestart && due != 0 && now.Ticks >= due;
    }

    private bool Exited => Interlocked.Read(ref _restartDueTicks) != 0;

    public static SlackRuntime Start(
        string database, Channel channel, string channelFingerprint, SlackSdk sdk, SlackTurns turns,
        ChannelChatsFactory chats, SlackOptions options,
        QuillLogger<SlackRuntime> logger, ChannelConnectionHealth? previous = null)
    {
        var runtime = new SlackRuntime(
            database, channel, channelFingerprint, sdk, turns, chats, options, logger, previous);

        runtime._run = Task.Run(runtime.ConnectAsync);
        return runtime;
    }

    public bool IsStopping => _stop.IsValueCreated;

    public Task StopAsync() => _stop.Value;

    private async Task StopCoreAsync()
    {
        await _cts.CancelAsync();
        await _replace.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        await Task.WhenAll(_run, _client.DisposeAsync().AsTask(), _chats.StopAsync())
            .ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        _cts.Dispose();
    }

    void SlackSdk.ISocketObserver.OnSocketOpened()
    {
        if (Exited || _cts.IsCancellationRequested)
            return;

        Health.MarkConnected();
        if (_logger.IsInfoEnabled)
            _logger.Info($"Slack socket connected for channel {_shortChannelId} (bot {_botUserId})");
    }

    void SlackSdk.ISocketObserver.OnSocketClosed(Exception? error)
    {
        if (_cts.IsCancellationRequested)
            return;

        var reason = error is null ? null : SlackApiErrors.Describe(error);
        Health.MarkDisconnected(reason);
        if (_logger.IsInfoEnabled)
            _logger.Info($"Slack socket closed for channel {_shortChannelId}" + (reason is null ? "" : $": {reason}"));
    }

    private async Task ConnectAsync()
    {
        try
        {
            await _client.Connect(new SocketModeConnectionOptions { NumberOfConnections = 1 }, _cts.Token);
        }
        catch (OperationCanceledException) when (_cts.IsCancellationRequested)
        {
        }
        catch (Exception e)
        {
            ExitAfterConnectFailure(e);
        }
    }

    internal void OnReconnectRetryFailed(Exception e)
    {
        if (_cts.IsCancellationRequested || Exited)
            return;

        var reason = SlackApiErrors.Describe(e);
        Health.MarkDisconnected(reason);
        if (_logger.IsWarnEnabled)
            _logger.Warn($"Slack socket for channel {_shortChannelId} failed to reconnect: {reason}");
    }

    // link_disabled is deliberately unhandled
    internal void OnReconnectGiveUp(Exception e)
    {
        if (_cts.IsCancellationRequested || Exited)
            return;

        if (ExitIfAppTokenRejected(e))
            return;

        Health.MarkDisconnected(SlackApiErrors.Describe(e));
        if (Interlocked.CompareExchange(ref _replacing, 1, 0) == 0)
            _replace = Task.Run(ReplaceSocketAsync);
    }

    private async Task ReplaceSocketAsync()
    {
        var delay = TimeSpan.Zero;
        try
        {
            while (_cts.IsCancellationRequested == false && Exited == false)
            {
                if (delay > TimeSpan.Zero)
                    await Task.Delay(delay, _cts.Token);

                var fresh = _sdk.NewSocketClient(_appToken, _handler, _netLogger, this);
                var old = Interlocked.Exchange(ref _client, fresh);
                await old.DisposeAsync().AsTask().ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
                if (_cts.IsCancellationRequested)
                {
                    await fresh.DisposeAsync();
                    return;
                }

                try
                {
                    await fresh.Connect(new SocketModeConnectionOptions { NumberOfConnections = 1 }, _cts.Token);
                    return;
                }
                catch (Exception e) when (e is not OperationCanceledException)
                {
                    if (ExitIfAppTokenRejected(e))
                        return;

                    var reason = SlackApiErrors.Describe(e);
                    Health.MarkDisconnected(reason);
                    delay = SlackApiErrors.RetryAfterOf(e) ?? NextDelay(delay);
                    if (_logger.IsWarnEnabled)
                        _logger.Warn($"Slack socket for channel {_shortChannelId} failed to reconnect: {reason}; retrying in {delay}");
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            Interlocked.Exchange(ref _replacing, 0);
        }
    }

    private static TimeSpan NextDelay(TimeSpan previous)
    {
        var next = previous < MinReconnectDelay ? MinReconnectDelay : previous * 2;
        return next > MaxReconnectDelay ? MaxReconnectDelay : next;
    }

    private bool ExitIfAppTokenRejected(Exception e)
    {
        var code = SlackApiErrors.ErrorCodeOf(e);
        var tokenError = SlackApiErrors.DescribeAppTokenError(code);
        if (tokenError is null)
            return false;

        _canRestart = SlackApiErrors.AppTokenErrorIsFixableInSlack(code);
        Exit(tokenError, _options.SocketRestartDelay);
        return true;
    }

    private void ExitAfterConnectFailure(Exception e)
    {
        if (ExitIfAppTokenRejected(e))
            return;

        Exit(SlackApiErrors.Describe(e), _options.SocketRestartDelay);
    }

    private void Exit(string error, TimeSpan restartDelay)
    {
        var due = (DateTime.UtcNow + restartDelay).Ticks;
        if (Interlocked.CompareExchange(ref _restartDueTicks, due, 0) != 0)
            return;

        Health.MarkDisconnected(error);
        if (_logger.IsWarnEnabled)
            _logger.Warn($"Slack socket for channel {_shortChannelId} exited: {error}");
    }
}
