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
    string? ChannelChangeVector { get; }

    ChannelConnectionHealth? Health => null;

    void CheckConnection()
    {
    }

    bool IsRestartDue(DateTime now) => false;

    Task StopAsync();
}

internal interface IChannelRuntimeFactory
{
    ChannelType Type { get; }

    bool CanStart(Channel channel);

    IChannelRuntime Start(string database, Channel channel, string? changeVector);
}

internal sealed class ChannelManager(
    IDocumentStore store,
    IEnumerable<IChannelRuntimeFactory> factories,
    IOptions<ApplianceOptions> options,
    IServerReady ready,
    QuillLogger<ChannelManager> logger) : BackgroundService
{
    private static readonly TimeSpan StopDrainTimeout = TimeSpan.FromSeconds(15);

    private readonly Dictionary<ChannelType, IChannelRuntimeFactory> _factories = factories.ToDictionary(f => f.Type);
    private readonly ConcurrentDictionary<(string Database, string ChannelId), IChannelRuntime> _runtimes = new();

    private volatile AsyncManualResetEvent? _wake;

    private volatile bool _stopped;

    public void Wake() => _wake?.Set();

    public ChannelConnectionHealth? HealthFor(string database, string channelId) =>
        _runtimes.TryGetValue((database, channelId), out var runtime) ? runtime.Health : null;

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
            (Channel Channel, string? ChangeVector, IChannelRuntimeFactory Factory)>();

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
                            (channel, session.Advanced.GetChangeVectorFor(channel), factory);
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

        foreach (var runtime in _runtimes.Values)
            runtime.CheckConnection();

        var now = DateTime.UtcNow;
        var stopping = new List<((string Database, string ChannelId) Key, IChannelRuntime Runtime)>();

        foreach (var (key, runtime) in _runtimes)
        {
            if (unreadable.Contains(key.Database))
                continue;

            if (desired.TryGetValue(key, out var current) && runtime.ChannelChangeVector == current.ChangeVector &&
                runtime.IsRestartDue(now) == false)
                continue;

            if (_runtimes.TryRemove(key, out _))
                stopping.Add((key, runtime));
        }

        await Task.WhenAll(stopping.Select(s => StopRuntimeAsync(s.Key, s.Runtime)));

        foreach (var (key, entry) in desired)
        {
            if (_stopped)
                return;

            if (_runtimes.ContainsKey(key))
                continue;

            try
            {
                _runtimes[key] = entry.Factory.Start(key.Database, entry.Channel, entry.ChangeVector);
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

    private async Task StopRuntimeAsync((string Database, string ChannelId) key, IChannelRuntime runtime)
    {
        try
        {
            await runtime.StopAsync();
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            _runtimes.TryAdd(key, runtime);
            if (logger.IsWarnEnabled)
                logger.Warn(
                    $"Runtime stop failed for channel {key.ChannelId} on {key.Database}: {e.Message}; " +
                    "keeping it to retry");
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

        try
        {
            await Task.WhenAll(runtimes.Select(r => r.StopAsync()))
                .WaitAsync(StopDrainTimeout, cancellationToken);
        }
        catch (Exception e) when (e is TimeoutException or OperationCanceledException)
        {
            if (logger.IsWarnEnabled)
                logger.Warn($"Channel runtimes did not drain within {StopDrainTimeout}");
        }
    }
}
