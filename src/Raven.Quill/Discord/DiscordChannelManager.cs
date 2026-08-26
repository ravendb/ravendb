using System.Collections.Concurrent;
using Microsoft.Extensions.Options;
using Raven.Client.Documents;
using Raven.Client.Exceptions.Database;
using Raven.Quill.Channels;
using Raven.Quill.Endpoints.Helpers;
using Raven.Quill.Hosting;
using Raven.Quill.Raven;
using Raven.Quill.Wizard;
using Sparrow.Server;

namespace Raven.Quill.Discord;

internal interface IDiscordChannelManager
{
    void Wake();
}

internal sealed class DiscordChannelManager(
    IDocumentStore store,
    DiscordInboundProcessor processor,
    DiscordHealthRegistry health,
    IServiceScopeFactory scopes,
    IOptions<ApplianceOptions> options,
    IServerReady ready,
    ILogger<DiscordChannelManager> logger) : BackgroundService, IDiscordChannelManager
{
    private readonly ConcurrentDictionary<(string Database, string ChannelId), DiscordGatewayRuntime> _runtimes = new();

    private volatile AsyncManualResetEvent? _wake;

    private volatile bool _stopped;

    public void Wake() => _wake?.Set();

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
                logger.LogWarning("Discord apply-changes pass failed: {Error}", e.Message);
            }

            await wake.WaitAsync(options.Value.Discord.ApplyChangesInterval);
        }
    }

    private async Task ApplyChangesAsync(CancellationToken ct)
    {
        var desired = new Dictionary<(string Database, string ChannelId), (Channel Channel, string? ChangeVector)>();

        var unreadable = new HashSet<string>();

        List<App> apps;
        try
        {
            using var session = store.OpenAsyncSession();
            apps = await session.LoadAllStartingWithAsync<App>(AppLookup.IdPrefix, ct);
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            logger.LogWarning("Discord apply-changes could not list apps: {Error}", e.Message);
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
                    if (channel is { Type: ChannelType.Discord, Enabled: true, Discord.BotToken.Length: > 0 })
                        desired[(app.Database, channel.ShortId)] =
                            (channel, session.Advanced.GetChangeVectorFor(channel));
                }
            }
            catch (DatabaseDoesNotExistException)
            {
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                unreadable.Add(app.Database);
                logger.LogWarning("Discord apply-changes skipped app {Slug}: {Error}", app.Slug, e.Message);
            }
        }

        foreach (var (key, runtime) in _runtimes)
        {
            if (unreadable.Contains(key.Database))
                continue;

            if (desired.TryGetValue(key, out var current) && runtime.ChannelChangeVector == current.ChangeVector &&
                IsRestartDue(runtime) == false)
                continue;

            if (_runtimes.TryRemove(key, out _) == false)
                continue;

            try
            {
                await runtime.StopAsync();
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                logger.LogWarning(
                    "Discord gateway stop failed for channel {ChannelId} on {Database}: {Error}",
                    key.ChannelId, key.Database, e.Message);
                continue;
            }

            logger.LogInformation(
                "Discord gateway stopped for channel {ChannelId} on {Database}", key.ChannelId, key.Database);
        }

        foreach (var (key, entry) in desired)
        {
            if (_stopped)
                return;

            if (_runtimes.ContainsKey(key))
                continue;

            try
            {
                _runtimes[key] = DiscordGatewayRuntime.Start(
                    key.Database, entry.Channel, entry.ChangeVector, processor, health, scopes,
                    options.Value.Discord, logger);
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                logger.LogWarning(
                    "Discord gateway failed to start for channel {ChannelId} on {Database}: {Error}",
                    key.ChannelId, key.Database, e.Message);
                continue;
            }

            logger.LogInformation(
                "Discord gateway starting for channel {ChannelId} (bot {BotUserId}) on {Database}",
                key.ChannelId, entry.Channel.Discord!.BotUserId, key.Database);
        }
    }

    private bool IsRestartDue(DiscordGatewayRuntime runtime) =>
        runtime.CanRestart && runtime.ExitedAt is { } exitedAt &&
        DateTime.UtcNow - exitedAt >= options.Value.Discord.GatewayRestartDelay;

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _stopped = true;
        await base.StopAsync(cancellationToken);

        var runtimes = _runtimes.Values.ToArray();
        _runtimes.Clear();

        try
        {
            await Task.WhenAll(runtimes.Select(r => r.StopAsync()))
                .WaitAsync(TimeSpan.FromSeconds(15), cancellationToken);
        }
        catch (Exception e) when (e is TimeoutException or OperationCanceledException)
        {
            logger.LogWarning("Discord runtimes did not drain within 15s");
        }
    }
}
