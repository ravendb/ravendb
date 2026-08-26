using Microsoft.Extensions.Options;
using Raven.Client.Documents;
using Raven.Quill.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Hosting;

namespace Raven.Quill.Discord;

internal sealed class DiscordInboundProcessor(
    IDocumentStore store,
    IAgentRouter router,
    IServiceScopeFactory scopes,
    DiscordHealthRegistry health,
    IOptions<ApplianceOptions> options,
    ILogger<DiscordInboundProcessor> logger) : IHostedService
{
    internal const string UnsupportedKindReply = "I can only read text messages right now.";
    internal const string ErrorReply = "Sorry - something went wrong handling that message. Please try again.";
    internal const string OverloadReply =
        "I'm still working through your earlier messages, so that one didn't make it. Please resend it once I've replied.";

    private const int DedupeCapacity = 4096;
    private static readonly TimeSpan DedupeTtl = TimeSpan.FromMinutes(15);
    private static readonly TimeSpan StopDrainTimeout = TimeSpan.FromSeconds(10);

    private sealed class SenderChain
    {
        public Task Tail = Task.CompletedTask;
        public int Pending;
        public bool OverloadNotified;
    }

    private readonly Dictionary<string, SenderChain> _senderChains = new();
    private readonly object _chainsLock = new();
    private readonly CancellationTokenSource _stopping = new();

    private readonly HashSet<string> _seenMessageIds = new(StringComparer.Ordinal);
    private readonly Queue<(string MessageId, DateTime SeenAt)> _seenOrder = new();
    private readonly object _dedupeLock = new();

    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        await _stopping.CancelAsync();

        Task[] tails;
        lock (_chainsLock)
            tails = _senderChains.Values.Select(c => c.Tail).ToArray();

        try
        {
            await Task.WhenAll(tails).WaitAsync(StopDrainTimeout, cancellationToken);
        }
        catch (TimeoutException)
        {
            logger.LogWarning("Discord sender chains did not drain within {Timeout}", StopDrainTimeout);
        }
    }

    public void Enqueue(
        string database, string channelId, string sender, string? senderUsername, string dmChannel,
        string messageId, string kind, string? text)
    {
        if (IsDuplicate(messageId))
            return;

        var chainKey = $"{database}/{Channel.ShortIdFor(channelId)}/{sender}";
        var notifyOverload = false;

        lock (_chainsLock)
        {
            if (_senderChains.TryGetValue(chainKey, out var chain) == false)
                _senderChains[chainKey] = chain = new SenderChain();

            if (chain.Pending >= options.Value.Discord.SenderQueueCapacity)
            {
                notifyOverload = chain.OverloadNotified == false;
                chain.OverloadNotified = true;
            }
            else
            {
                chain.Pending++;
                var next = chain.Tail
                    .ContinueWith(
                        _ => HandleMessageSafeAsync(database, channelId, sender, senderUsername, dmChannel, kind, text),
                        CancellationToken.None, TaskContinuationOptions.None, TaskScheduler.Default)
                    .Unwrap();
                chain.Tail = next;

                next.ContinueWith(_ => OnTurnCompleted(chainKey, chain), TaskScheduler.Default);
            }
        }

        if (notifyOverload)
            _ = SendOverloadNoticeAsync(database, channelId, dmChannel);
    }

    private void OnTurnCompleted(string chainKey, SenderChain chain)
    {
        lock (_chainsLock)
        {
            chain.Pending--;
            if (chain.Pending > 0)
                return;

            chain.OverloadNotified = false;
            if (_senderChains.TryGetValue(chainKey, out var current) && current == chain)
                _senderChains.Remove(chainKey);
        }
    }

    private bool IsDuplicate(string messageId)
    {
        if (messageId.Length == 0)
            return false;

        lock (_dedupeLock)
        {
            var now = DateTime.UtcNow;
            while (_seenOrder.Count > 0 &&
                   (_seenOrder.Count > DedupeCapacity || now - _seenOrder.Peek().SeenAt > DedupeTtl))
                _seenMessageIds.Remove(_seenOrder.Dequeue().MessageId);

            if (_seenMessageIds.Add(messageId) == false)
                return true;

            _seenOrder.Enqueue((messageId, now));
            return false;
        }
    }

    private async Task HandleMessageSafeAsync(
        string database, string channelId, string sender, string? senderUsername, string dmChannel, string kind,
        string? text)
    {
        try
        {
            await HandleMessageAsync(
                database, channelId, sender, senderUsername, dmChannel, kind, text, _stopping.Token);
        }
        catch (OperationCanceledException) when (_stopping.IsCancellationRequested)
        {
        }
        catch (Exception e)
        {
            logger.LogWarning(
                "Discord message handling failed for channel {ChannelId} sender {Sender}: {Error}",
                channelId, sender, e.Message);
        }
    }

    private async Task HandleMessageAsync(
        string database, string channelId, string sender, string? senderUsername, string dmChannel, string kind,
        string? text, CancellationToken ct)
    {
        Channel? channel;
        using (var session = store.OpenAsyncSession(database))
            channel = await session.LoadAsync<Channel>(channelId, ct);

        if (channel is not { Type: ChannelType.Discord, Enabled: true, Discord: { } settings })
            return;

        var shortChannelId = channel.ShortId;

        await using var scope = scopes.CreateAsyncScope();
        var discord = scope.ServiceProvider.GetRequiredService<IDiscordClient>();

        if (kind != "text")
        {
            await TrySendAsync(discord, database, shortChannelId, settings, dmChannel, UnsupportedKindReply, ct);
            return;
        }

        var prompt = (text ?? "").Trim();
        if (prompt.Length == 0)
            return;

        var conversationId = DiscordConversationId.ForUtcDay(shortChannelId, sender, DateTime.UtcNow);

        var config = await AgentLookup.FindAsync(store, database, channel.AgentId, ct);
        if (config is null)
            throw new InvalidOperationException($"agent '{channel.AgentId}' is no longer registered in this app");

        var (parameters, bindError) = DiscordParameterBindings.Bind(
            config, settings.ParameterBindings, sender, senderUsername);
        if (parameters is null)
        {
            await TrySendAsync(discord, database, shortChannelId, settings, dmChannel, ErrorReply, ct);
            throw new InvalidOperationException(bindError);
        }

        var reply = new DiscordStreamingReply(
            discord, settings.BotToken, dmChannel, options.Value.Discord, logger, ct);

        try
        {
            await router.RunAsync(
                new AgentRequest(database, config.Identifier, conversationId, prompt, channel.Id!,
                    parameters.ToDictionary(
                        parameter => parameter.Key,
                        parameter => AgentParameterValue.FromString(parameter.Value))),
                reply.OnChunkAsync, config, ct);

            await reply.FinalizeAsync();
            health.RecordSendSuccess(database, shortChannelId);

            if (reply.IsEmpty)
                logger.LogWarning("Discord agent turn produced an empty reply for channel {ChannelId}", channel.Id);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception e)
        {
            await TrySendAsync(discord, database, shortChannelId, settings, dmChannel, ErrorReply, ct);

            if (e is DiscordApiException apiError)
                health.RecordSendError(database, shortChannelId, apiError.Message);

            throw;
        }
    }

    private async Task SendOverloadNoticeAsync(string database, string channelId, string dmChannel)
    {
        try
        {
            Channel? channel;
            using (var session = store.OpenAsyncSession(database))
                channel = await session.LoadAsync<Channel>(channelId, _stopping.Token);

            if (channel is not { Type: ChannelType.Discord, Enabled: true, Discord: { } settings })
                return;

            await using var scope = scopes.CreateAsyncScope();
            var discord = scope.ServiceProvider.GetRequiredService<IDiscordClient>();
            await TrySendAsync(
                discord, database, channel.ShortId, settings, dmChannel, OverloadReply, _stopping.Token);
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            logger.LogDebug("Discord overload notice failed for channel {ChannelId}: {Error}", channelId, e.Message);
        }
    }

    private async Task TrySendAsync(
        IDiscordClient discord, string database, string shortChannelId, DiscordSettings settings, string dmChannel,
        string text, CancellationToken ct)
    {
        try
        {
            await discord.CreateMessageAsync(settings.BotToken, dmChannel, text, ct);
            health.RecordSendSuccess(database, shortChannelId);
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            if (e is DiscordApiException apiError)
                health.RecordSendError(database, shortChannelId, apiError.Message);

            logger.LogWarning(
                "Discord send failed for channel {ChannelId} in {DmChannel}: {Error}",
                shortChannelId, dmChannel, e.Message);
        }
    }
}
