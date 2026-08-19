using Microsoft.Extensions.Options;
using Raven.Client.Documents;
using Raven.Quill.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Hosting;

namespace Raven.Quill.Slack;

internal sealed class SlackInboundProcessor(
    IDocumentStore store,
    IAgentRouter router,
    ISlackClient slack,
    SlackHealthRegistry health,
    IOptions<ApplianceOptions> options,
    ILogger<SlackInboundProcessor> logger)
{
    internal const string UnsupportedKindReply = "I can only read text messages right now.";
    internal const string ErrorReply = "Sorry - something went wrong handling that message. Please try again.";

    private const int DedupeCapacity = 4096;
    private static readonly TimeSpan DedupeTtl = TimeSpan.FromMinutes(15);

    private readonly Dictionary<string, Task> _senderChains = new();
    private readonly object _chainsLock = new();

    private readonly HashSet<string> _seenEventIds = new(StringComparer.Ordinal);
    private readonly Queue<(string EventId, DateTime SeenAt)> _seenOrder = new();
    private readonly object _dedupeLock = new();

    public void Enqueue(
        string database, string channelId, string sender, string dmChannel, string eventId, string kind, string? text)
    {
        if (IsDuplicate(eventId))
            return;

        var chainKey = $"{database}/{ShortChannelId(channelId)}/{sender}";

        lock (_chainsLock)
        {
            var tail = _senderChains.GetValueOrDefault(chainKey) ?? Task.CompletedTask;
            var next = tail
                .ContinueWith(_ => HandleMessageSafeAsync(database, channelId, sender, dmChannel, kind, text),
                    CancellationToken.None, TaskContinuationOptions.None, TaskScheduler.Default)
                .Unwrap();
            _senderChains[chainKey] = next;

            next.ContinueWith(_ =>
            {
                lock (_chainsLock)
                {
                    if (_senderChains.TryGetValue(chainKey, out var current) && current == next)
                        _senderChains.Remove(chainKey);
                }
            }, TaskScheduler.Default);
        }
    }

    private bool IsDuplicate(string eventId)
    {
        if (eventId.Length == 0)
            return false;

        lock (_dedupeLock)
        {
            var now = DateTime.UtcNow;
            while (_seenOrder.Count > 0 &&
                   (_seenOrder.Count > DedupeCapacity || now - _seenOrder.Peek().SeenAt > DedupeTtl))
                _seenEventIds.Remove(_seenOrder.Dequeue().EventId);

            if (_seenEventIds.Add(eventId) == false)
                return true;

            _seenOrder.Enqueue((eventId, now));
            return false;
        }
    }

    private async Task HandleMessageSafeAsync(
        string database, string channelId, string sender, string dmChannel, string kind, string? text)
    {
        try
        {
            await HandleMessageAsync(database, channelId, sender, dmChannel, kind, text, CancellationToken.None);
        }
        catch (Exception e)
        {
            logger.LogWarning(
                "Slack message handling failed for channel {ChannelId} sender {Sender}: {Error}",
                channelId, sender, e.Message);
        }
    }

    private async Task HandleMessageAsync(
        string database, string channelId, string sender, string dmChannel, string kind, string? text, CancellationToken ct)
    {
        Channel? channel;
        using (var session = store.OpenAsyncSession(database))
            channel = await session.LoadAsync<Channel>(channelId, ct);

        if (channel is not { Type: ChannelType.Slack, Enabled: true, Slack: { } settings })
            return;

        var shortChannelId = channel.ShortId;

        if (kind != "text")
        {
            await TrySendAsync(database, shortChannelId, settings, dmChannel, UnsupportedKindReply, ct);
            return;
        }

        var prompt = (text ?? "").Trim();
        if (prompt.Length == 0)
            return;

        var conversationId = SlackConversationId.ForUtcDay(shortChannelId, sender, DateTime.UtcNow);

        var config = await AgentLookup.FindAsync(store, database, channel.AgentId, ct);
        if (config is null)
            throw new InvalidOperationException($"agent '{channel.AgentId}' is no longer registered in this app");

        if (SlackParameterBindings.TryBind(
                config, settings.ParameterBindings, sender, out var parameters, out var bindError) == false)
        {
            await TrySendAsync(database, shortChannelId, settings, dmChannel, ErrorReply, ct);
            throw new InvalidOperationException(bindError);
        }

        var reply = new SlackStreamingReply(slack, settings.BotToken, dmChannel, options.Value.Slack, logger, ct);

        try
        {
            await router.RunAsync(
                new AgentRequest(database, config.Identifier, conversationId, prompt, channel.Id!, parameters),
                reply.OnChunkAsync, config, ct);

            await reply.FinalizeAsync();

            if (reply.IsEmpty)
                logger.LogWarning("Slack agent turn produced an empty reply for channel {ChannelId}", channel.Id);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception e)
        {
            if (e is SlackApiException apiError)
                health.RecordSendError(database, shortChannelId, apiError.Message);

            await TrySendAsync(database, shortChannelId, settings, dmChannel, ErrorReply, ct);
            throw;
        }
    }

    private async Task TrySendAsync(
        string database, string shortChannelId, SlackSettings settings, string dmChannel, string text, CancellationToken ct)
    {
        try
        {
            await slack.PostMessageAsync(settings.BotToken, dmChannel, text, ct);
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            if (e is SlackApiException apiError)
                health.RecordSendError(database, shortChannelId, apiError.Message);

            logger.LogWarning(
                "Slack send failed for channel {ChannelId} in {DmChannel}: {Error}",
                shortChannelId, dmChannel, e.Message);
        }
    }

    private static string ShortChannelId(string channelId) =>
        channelId.StartsWith(Channel.IdPrefix, StringComparison.Ordinal)
            ? channelId[Channel.IdPrefix.Length..]
            : channelId;
}
