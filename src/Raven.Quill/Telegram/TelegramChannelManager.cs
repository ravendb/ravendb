using System.Collections.Concurrent;
using Microsoft.Extensions.Options;
using Raven.Client.Documents;
using Raven.Quill.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Endpoints.Helpers;
using Raven.Quill.Hosting;
using Raven.Quill.Raven;
using Raven.Quill.Wizard;

namespace Raven.Quill.Telegram;

internal interface ITelegramChannelManager
{
    Task StartOrRestartAsync(string database, Channel channel);

    Task StopAsync(string database, string channelId);

    Task StopAllForDatabaseAsync(string database);

    IReadOnlyDictionary<string, TelegramChannelHealthSnapshot> GetHealth(string database);
}

internal sealed class TelegramChannelManager(
    IDocumentStore store,
    ITelegramBotClientFactory botFactory,
    IAgentRouter router,
    IOptions<ApplianceOptions> options,
    IServerReady ready,
    ILogger<TelegramChannelManager> logger) : BackgroundService, ITelegramChannelManager
{
    private readonly ConcurrentDictionary<(string Database, string ChannelId), TelegramChannelPoller> _pollers = new();

    private readonly SemaphoreSlim _transition = new(1, 1);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (ready.IsReady == false)
            await Task.Delay(250, stoppingToken);

        await ReconcileAsync(stoppingToken);
    }

    private async Task ReconcileAsync(CancellationToken ct)
    {
        List<App> apps;
        try
        {
            using var session = store.OpenAsyncSession();
            apps = await session.LoadAllStartingWithAsync<App>(AppLookup.IdPrefix, ct);
        }
        catch (Exception e)
        {
            logger.LogWarning("Telegram reconciliation could not list apps: {Error}", e.Message);
            return;
        }

        foreach (var app in apps)
        {
            if (ct.IsCancellationRequested)
                return;

            try
            {
                List<Channel> channels;
                using (var session = store.OpenAsyncSession(app.Database))
                    channels = await session.LoadAllStartingWithAsync<Channel>(Channel.IdPrefix, ct);

                foreach (var channel in channels)
                {
                    if (channel is not { Type: ChannelType.Telegram, Enabled: true, Telegram.BotToken.Length: > 0 })
                        continue;

                    await StartOrRestartAsync(app.Database, channel);
                }
            }
            catch (Exception e)
            {
                logger.LogWarning(
                    "Telegram reconciliation skipped app {Slug}: {Error}", app.Slug, e.Message);
            }
        }
    }

    public async Task StartOrRestartAsync(string database, Channel channel)
    {
        var channelId = Channel.StripIdPrefix(channel.Id);
        var key = (database, channelId);

        await _transition.WaitAsync();
        try
        {
            if (_pollers.TryRemove(key, out var existing))
                await existing.StopAsync();

            var bot = botFactory.Create(channel.Telegram!.BotToken);
            var poller = new TelegramChannelPoller(database, channel, bot, store, router, options.Value, logger);
            _pollers[key] = poller;
            poller.Start();

            logger.LogInformation(
                "Telegram poller started for channel {ChannelId} (bot {Bot}) on {Database}",
                channelId, TelegramSettings.RedactToken(channel.Telegram.BotToken), database);
        }
        finally
        {
            _transition.Release();
        }
    }

    public async Task StopAsync(string database, string channelId)
    {
        await _transition.WaitAsync();
        try
        {
            if (_pollers.TryRemove((database, channelId), out var poller))
            {
                await poller.StopAsync();
                logger.LogInformation(
                    "Telegram poller stopped for channel {ChannelId} on {Database}", channelId, database);
            }
        }
        finally
        {
            _transition.Release();
        }
    }

    public async Task StopAllForDatabaseAsync(string database)
    {
        foreach (var key in _pollers.Keys.Where(k => k.Database == database).ToArray())
            await StopAsync(key.Database, key.ChannelId);
    }

    public IReadOnlyDictionary<string, TelegramChannelHealthSnapshot> GetHealth(string database) =>
        _pollers
            .Where(kvp => kvp.Key.Database == database)
            .ToDictionary(kvp => kvp.Key.ChannelId, kvp => kvp.Value.Health.Snapshot(isPolling: true));

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        foreach (var poller in _pollers.Values.ToArray())
            await poller.StopAsync();
        _pollers.Clear();

        await base.StopAsync(cancellationToken);
    }
}
