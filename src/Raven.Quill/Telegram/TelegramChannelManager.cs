using System.Collections.Concurrent;
using Microsoft.Extensions.Options;
using Raven.Client.Documents;
using Raven.Client.Exceptions.Database;
using Raven.Quill.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Endpoints.Helpers;
using Raven.Quill.Hosting;
using Raven.Quill.Raven;
using Raven.Quill.Wizard;
using Sparrow.Server;
using Telegram.Bot;
using Telegram.Bot.Exceptions;
using TelegramUser = Telegram.Bot.Types.User;

using Raven.Quill.Logging;

namespace Raven.Quill.Telegram;

internal interface ITelegramChannelManager
{
    Task<(TelegramUser? Bot, string? Error)> ValidateBotTokenAsync(string botToken, CancellationToken ct);

    void Wake();
}

internal sealed class TelegramChannelManager(
    IDocumentStore store,
    ITelegramBotClientFactory botFactory,
    IAgentRouter router,
    IOptions<ApplianceOptions> options,
    IServerReady ready,
    QuillLogger<TelegramChannelManager> logger) : BackgroundService, ITelegramChannelManager
{
    private readonly ConcurrentDictionary<(string Database, string ChannelId), TelegramBotRuntime> _bots = new();

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
                if (logger.IsWarnEnabled)
                    logger.Warn($"Telegram apply-changes pass failed: {e.Message}");
            }

            await wake.WaitAsync(options.Value.Telegram.ApplyChangesInterval);
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
            if (logger.IsWarnEnabled)
                logger.Warn($"Telegram apply-changes could not list apps: {e.Message}");
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
                    if (channel is { Type: ChannelType.Telegram, Enabled: true, Telegram.BotToken.Length: > 0 })
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
                if (logger.IsWarnEnabled)
                    logger.Warn($"Telegram apply-changes skipped app {app.Slug}: {e.Message}");
            }
        }

        foreach (var (key, bot) in _bots)
        {
            if (unreadable.Contains(key.Database))
                continue;

            if (desired.TryGetValue(key, out var current) && bot.ChannelChangeVector == current.ChangeVector)
                continue;

            if (_bots.TryRemove(key, out _) == false)
                continue;

            try
            {
                await bot.StopAsync();
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                if (logger.IsWarnEnabled)
                    logger.Warn(
                        $"Telegram bot stop failed for channel {key.ChannelId} on {key.Database}: {e.Message}");
                continue;
            }

            if (logger.IsInfoEnabled)
                logger.Info($"Telegram bot stopped for channel {key.ChannelId} on {key.Database}");
        }

        foreach (var (key, entry) in desired)
        {
            if (_stopped)
                return;

            if (_bots.ContainsKey(key))
                continue;

            try
            {
                _bots[key] = TelegramBotRuntime.Start(
                    key.Database, entry.Channel, entry.ChangeVector, botFactory, store, router, options.Value, logger);
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                // an unstartable channel doc must not starve the entries after it
                if (logger.IsWarnEnabled)
                    logger.Warn(
                        $"Telegram bot failed to start for channel {key.ChannelId} on {key.Database}: " +
                        $"{e.Message}");
                continue;
            }

            if (logger.IsInfoEnabled)
                logger.Info(
                    $"Telegram bot started for channel {key.ChannelId} " +
                    $"(bot @{entry.Channel.Telegram!.BotUsername}) on {key.Database}");
        }
    }

    public async Task<(TelegramUser? Bot, string? Error)> ValidateBotTokenAsync(string botToken, CancellationToken ct)
    {
        try
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
            timeout.CancelAfter(TimeSpan.FromSeconds(10));
            var bot = await botFactory.Create(botToken).GetMe(timeout.Token);
            return (bot, null);
        }
        catch (ArgumentException)
        {
            // Telegram.Bot rejects the token shape in the client constructor, before any HTTP call
            return (null, "invalid bot token format; expected '<botId>:<secret>' as issued by @BotFather");
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested == false)
        {
            return (null, "telegram did not respond while validating the bot token");
        }
        catch (ApiRequestException e)
        {
            return (null, $"telegram rejected the bot token: {e.Message}");
        }
        catch (RequestException e)
        {
            return (null, $"could not reach telegram: {e.Message}");
        }
    }

    internal int GetActiveChatCount(string database, string channelId) =>
        _bots.TryGetValue((database, channelId), out var bot) ? bot.ActiveChatCount : 0;

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _stopped = true;
        await base.StopAsync(cancellationToken);

        var bots = _bots.Values.ToArray();
        _bots.Clear();

        try
        {
            await Task.WhenAll(bots.Select(b => b.StopAsync())).WaitAsync(TimeSpan.FromSeconds(10), cancellationToken);
        }
        catch (Exception e) when (e is TimeoutException or OperationCanceledException)
        {
            if (logger.IsWarnEnabled)
                logger.Warn("Telegram runtime did not drain within 10s");
        }
    }
}
