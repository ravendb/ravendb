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
using Telegram.Bot;
using Telegram.Bot.Exceptions;
using TelegramUser = Telegram.Bot.Types.User;

namespace Raven.Quill.Telegram;

internal interface ITelegramChannelManager
{
    Task<(TelegramUser? Bot, string? Error)> ValidateBotTokenAsync(string botToken, CancellationToken ct);

    /// Asks for an apply-changes pass now instead of at the next tick. Callers persist their channel
    /// documents first and never start or stop a bot themselves.
    void Wake();

    IReadOnlyDictionary<string, TelegramChannelHealthSnapshot> GetHealth(string database);
}

/// Converges the running bots to the enabled Telegram channel documents. Desired state lives in the DB;
/// every pass is a full diff, so a failed pass is retried by the next one and endpoint/delete races
/// heal themselves.
internal sealed class TelegramChannelManager(
    IDocumentStore store,
    ITelegramBotClientFactory botFactory,
    IAgentRouter router,
    IOptions<ApplianceOptions> options,
    IServerReady ready,
    ILogger<TelegramChannelManager> logger) : BackgroundService, ITelegramChannelManager
{
    private readonly ConcurrentDictionary<(string Database, string ChannelId), TelegramBotRuntime> _bots = new();

    private readonly AsyncWakeSignal _wake = new();

    public void Wake() => _wake.Set();

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (ready.IsReady == false)
            await Task.Delay(250, stoppingToken);

        while (stoppingToken.IsCancellationRequested == false)
        {
            // reset before the pass: a Set() arriving while it runs lands on the fresh source, so the
            // next wait returns immediately instead of losing the wakeup
            _wake.Reset();

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
                logger.LogWarning("Telegram apply-changes pass failed: {Error}", e.Message);
            }

            try
            {
                await _wake.WaitAsync(options.Value.TelegramApplyChangesInterval, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    private async Task ApplyChangesAsync(CancellationToken ct)
    {
        var desired = new Dictionary<(string Database, string ChannelId), Channel>();

        // apps whose desired state we could not read this pass; their bots are left alone, because a
        // read blip must never stop a healthy bot
        var unreadable = new HashSet<string>();

        List<App> apps;
        try
        {
            using var session = store.OpenAsyncSession();
            apps = await session.LoadAllStartingWithAsync<App>(AppLookup.IdPrefix, ct);
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            // no authority over any app this pass; every bot keeps running and the next tick retries
            logger.LogWarning("Telegram apply-changes could not list apps: {Error}", e.Message);
            return;
        }

        foreach (var app in apps)
        {
            ct.ThrowIfCancellationRequested();

            try
            {
                List<Channel> channels;
                using (var session = store.OpenAsyncSession(app.Database))
                    channels = await session.LoadAllStartingWithAsync<Channel>(Channel.IdPrefix, ct);

                foreach (var channel in channels)
                {
                    if (channel is { Type: ChannelType.Telegram, Enabled: true, Telegram.BotToken.Length: > 0 })
                        desired[(app.Database, Channel.StripIdPrefix(channel.Id))] = channel;
                }
            }
            catch (DatabaseDoesNotExistException)
            {
                // definitive answer: the app's database is gone. leaving it out of `unreadable` is what
                // lets the sweep below stop its bots.
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                unreadable.Add(app.Database);
                logger.LogWarning("Telegram apply-changes skipped app {Slug}: {Error}", app.Slug, e.Message);
            }
        }

        // stop whatever should not run: disabled, deleted, app gone, or token rotated. an app whose
        // document was deleted is absent from `apps`, so its bots have no desired entry and land here.
        foreach (var (key, bot) in _bots)
        {
            if (unreadable.Contains(key.Database))
                continue;

            if (desired.TryGetValue(key, out var current) && bot.BotToken == current.Telegram!.BotToken)
                continue;

            if (_bots.TryRemove(key, out _) == false)
                continue;

            await bot.StopAsync();
            logger.LogInformation(
                "Telegram bot stopped for channel {ChannelId} on {Database}", key.ChannelId, key.Database);
        }

        // only the token is compared above, so reassigning the agent or editing parameters never
        // restarts a bot and never kills a live chat
        foreach (var (key, channel) in desired)
        {
            if (_bots.ContainsKey(key))
                continue;

            _bots[key] = TelegramBotRuntime.Start(
                key.Database, channel, botFactory, store, router, options.Value, logger);

            logger.LogInformation(
                "Telegram bot started for channel {ChannelId} (bot {Bot}) on {Database}",
                key.ChannelId, TelegramSettings.RedactToken(channel.Telegram!.BotToken), key.Database);
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
            return (null, $"telegram rejected the bot token: {TelegramSettings.ScrubToken(e.Message, botToken)}");
        }
        catch (RequestException e)
        {
            return (null, $"could not reach telegram: {TelegramSettings.ScrubToken(e.Message, botToken)}");
        }
    }

    public IReadOnlyDictionary<string, TelegramChannelHealthSnapshot> GetHealth(string database) =>
        _bots
            .Where(kvp => kvp.Key.Database == database)
            .ToDictionary(kvp => kvp.Key.ChannelId, kvp => kvp.Value.Health.Snapshot());

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        var bots = _bots.Values.ToArray();
        _bots.Clear();

        try
        {
            await Task.WhenAll(bots.Select(b => b.StopAsync())).WaitAsync(TimeSpan.FromSeconds(10), cancellationToken);
        }
        catch (Exception e) when (e is TimeoutException or OperationCanceledException)
        {
            logger.LogWarning("Telegram runtime did not drain within 10s");
        }

        await base.StopAsync(cancellationToken);
    }
}
