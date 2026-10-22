using System.Collections.Concurrent;
using Microsoft.Extensions.Options;
using Raven.Client.Documents;
using Raven.Client.Exceptions.Database;
using Raven.Quill.Endpoints.Helpers;
using Raven.Quill.Hosting;
using Raven.Quill.Raven;
using Raven.Quill.Wizard;
using Sparrow.Server;

using Raven.Quill.Logging;

namespace Raven.Quill.Channels;

internal interface IChannelRuntime
{
    string ChannelFingerprint { get; }

    ChannelConnectionHealth? Health => null;

    bool IsConnected => Health?.IsConnected ?? false;

    bool IsRestartDue(DateTime now) => false;

    Task StopAsync();
}

internal interface IChannelRuntimeFactory
{
    ChannelType Type { get; }

    bool CanStart(Channel channel);

    IChannelRuntime Start(string database, Channel channel, string fingerprint, ChannelConnectionHealth? previous);
}

internal sealed class ChannelManager(
    IDocumentStore store,
    IEnumerable<IChannelRuntimeFactory> factories,
    IOptions<ApplianceOptions> options,
    IServerReady ready,
    QuillLogger<ChannelManager> logger) : BackgroundService
{
    private readonly Dictionary<ChannelType, IChannelRuntimeFactory> _factories = factories.ToDictionary(f => f.Type);
    private readonly ConcurrentDictionary<(string Database, string ChannelId), IChannelRuntime> _runtimes = new();
    private readonly ConcurrentDictionary<(string Database, string ChannelId), Task> _draining = new();

    private volatile AsyncManualResetEvent? _wake;

    private volatile bool _stopped;

    public void Wake() => _wake?.Set();

    public ChannelConnectionHealth? HealthFor(string database, string channelId) => RuntimeFor(database, channelId)?.Health;

    public bool IsConnectedFor(string database, string channelId) => RuntimeFor(database, channelId)?.IsConnected ?? false;

    internal IChannelRuntime? RuntimeFor(string database, string channelId) =>
        _runtimes.GetValueOrDefault((database, channelId));

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var wake = new AsyncManualResetEvent(stoppingToken);
        _wake = wake;

        while (ready.IsReady == false)
            await Task.Delay(250, stoppingToken);

        while (stoppingToken.IsCancellationRequested == false)
        {
            wake.Reset();

            try
            {
                await ApplyChangesAsync(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception e)
            {
                if (logger.IsWarnEnabled)
                    logger.Warn($"Channel apply-changes pass failed: {e.Message}");
            }

            await wake.WaitAsync(options.Value.ChannelApplyChangesInterval);
        }
    }

    private async Task ApplyChangesAsync(CancellationToken ct)
    {
        var desired = new Dictionary<(string Database, string ChannelId),
            (Channel Channel, string Fingerprint, IChannelRuntimeFactory Factory)>();

        var unreadable = new HashSet<string>();

        List<App> apps;
        try
        {
            using var session = store.OpenAsyncSession();
            apps = await session.LoadAllStartingWithAsync<App>(AppLookup.IdPrefix, ct);
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            if (logger.IsWarnEnabled)
                logger.Warn($"Channel apply-changes could not list apps: {e.Message}");
            return;
        }

        foreach (var app in apps)
        {
            ct.ThrowIfCancellationRequested();

            try
            {
                using var session = store.OpenAsyncSession(app.Database);
                var channels = await session.LoadAllStartingWithAsync<Channel>(Channel.IdPrefix, ct);

                foreach (var channel in channels)
                {
                    if (channel.Enabled && _factories.TryGetValue(channel.Type, out var factory) &&
                        factory.CanStart(channel))
                        desired[(app.Database, channel.ShortId)] =
                            (channel, ChannelRuntimeFingerprint.Of(channel), factory);
                }
            }
            catch (DatabaseDoesNotExistException)
            {
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                unreadable.Add(app.Database);
                if (logger.IsWarnEnabled)
                    logger.Warn($"Channel apply-changes skipped app {app.Slug}: {e.Message}");
            }
        }

        var now = DateTime.UtcNow;
        var stopping = new List<((string Database, string ChannelId) Key, IChannelRuntime Runtime)>();
        var restarting = new Dictionary<(string Database, string ChannelId), ChannelConnectionHealth>();

        foreach (var (key, runtime) in _runtimes)
        {
            if (unreadable.Contains(key.Database))
                continue;

            var unchanged = desired.TryGetValue(key, out var current) && runtime.ChannelFingerprint == current.Fingerprint;
            if (unchanged && runtime.IsRestartDue(now) == false)
                continue;

            if (_runtimes.TryRemove(key, out _) == false)
                continue;

            stopping.Add((key, runtime));
            if (unchanged && runtime.Health is { } health)
                restarting[key] = health;
        }

        await Task.WhenAll(stopping.Select(s => StopRuntimeAsync(s.Key, s.Runtime)));

        foreach (var (key, entry) in desired)
        {
            if (_stopped)
                return;

            if (_runtimes.ContainsKey(key) || IsDraining(key))
                continue;

            try
            {
                _runtimes[key] = entry.Factory.Start(
                    key.Database, entry.Channel, entry.Fingerprint, restarting.GetValueOrDefault(key));
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                if (logger.IsWarnEnabled)
                    logger.Warn(
                        $"{entry.Channel.Type} runtime failed to start for channel {key.ChannelId} on {key.Database}: " +
                        $"{e.Message}");
                continue;
            }

            if (logger.IsInfoEnabled)
                logger.Info($"{entry.Channel.Type} runtime started for channel {key.ChannelId} on {key.Database}");
        }
    }

    private bool IsDraining((string Database, string ChannelId) key)
    {
        if (_draining.TryGetValue(key, out var stop) == false)
            return false;

        if (stop.IsCompleted == false)
            return true;

        _draining.TryRemove(new KeyValuePair<(string Database, string ChannelId), Task>(key, stop));
        return false;
    }

    private async Task StopRuntimeAsync((string Database, string ChannelId) key, IChannelRuntime runtime)
    {
        var stop = runtime.StopAsync();
        try
        {
            await stop.WaitAsync(options.Value.ChannelRuntimeStopTimeout);
        }
        catch (TimeoutException)
        {
            _draining[key] = stop;
            if (logger.IsWarnEnabled)
                logger.Warn(
                    $"Runtime for channel {key.ChannelId} on {key.Database} did not stop within " +
                    $"{options.Value.ChannelRuntimeStopTimeout}; its successor waits until it does");
            return;
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            if (logger.IsWarnEnabled)
                logger.Warn(
                    $"Runtime for channel {key.ChannelId} on {key.Database} did not stop: {e.Message}; giving up on it");
            return;
        }

        if (logger.IsInfoEnabled)
            logger.Info($"Runtime stopped for channel {key.ChannelId} on {key.Database}");
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _stopped = true;
        await base.StopAsync(cancellationToken);

        var runtimes = _runtimes.Values.ToArray();
        _runtimes.Clear();

        var timeout = options.Value.ChannelRuntimeStopTimeout;
        try
        {
            await Task.WhenAll(runtimes.Select(r => r.StopAsync()))
                .WaitAsync(timeout, cancellationToken);
        }
        catch (Exception e) when (e is TimeoutException or OperationCanceledException)
        {
            if (logger.IsWarnEnabled)
                logger.Warn($"Channel runtimes did not drain within {timeout}");
        }
    }
}
