using System.Text;
using Raven.Client.Documents;
using Raven.Quill.Agents;
using Raven.Quill.Channels;
using Raven.Quill.Metrics;

namespace Raven.Quill.WhatsApp;

/// Runs inbound WhatsApp messages through the agent and replies via the bridge.
/// The HTTP endpoint answers 202 immediately (agent turns can take minutes); work
/// continues here on per-sender chains so one sender's turns never interleave.
internal sealed class WhatsAppInboundProcessor(
    IDocumentStore store,
    IAgentRouter router,
    IWhatsAppBridgeClient bridge,
    ILogger<WhatsAppInboundProcessor> logger)
{
    internal const string UnsupportedKindReply = "I can only read text messages right now.";
    internal const string ErrorReply = "Sorry - something went wrong handling that message. Please try again.";
    internal const string ConversationClearedReply = "Conversation cleared. The next message starts a fresh one.";

    private readonly Dictionary<string, Task> _senderChains = new();
    private readonly object _chainsLock = new();

    public void Enqueue(string database, Channel channel, string sender, string kind, string? text)
    {
        var chainKey = $"{database}/{channel.ShortId}/{WhatsAppConversationId.SenderDigits(sender)}";

        lock (_chainsLock)
        {
            var tail = _senderChains.GetValueOrDefault(chainKey) ?? Task.CompletedTask;
            var next = tail
                .ContinueWith(_ => HandleMessageSafeAsync(database, channel, sender, kind, text),
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

    private async Task HandleMessageSafeAsync(string database, Channel channel, string sender, string kind, string? text)
    {
        try
        {
            await HandleMessageAsync(database, channel, sender, kind, text, CancellationToken.None);
        }
        catch (Exception e)
        {
            logger.LogWarning(
                "WhatsApp message handling failed for channel {ChannelId} sender ...{SenderSuffix}: {Error}",
                channel.Id, SenderSuffix(sender), e.Message);
        }
    }

    private async Task HandleMessageAsync(
        string database, Channel channel, string sender, string kind, string? text, CancellationToken ct)
    {
        var channelId = channel.ShortId;

        if (kind != "text")
        {
            await TrySendAsync(database, channelId, sender, UnsupportedKindReply, ct);
            return;
        }

        var prompt = (text ?? "").Trim();
        if (prompt.Length == 0)
            return;

        var conversationId = WhatsAppConversationId.For(channelId, sender, DateTime.UtcNow);

        if (IsClearCommand(prompt))
        {
            await ClearConversationAsync(database, conversationId, ct);
            await TrySendAsync(database, channelId, sender, ConversationClearedReply, ct);
            return;
        }

        var config = await AgentLookup.FindAsync(store, database, channel.AgentId, ct);
        if (config is null)
            throw new InvalidOperationException($"agent '{channel.AgentId}' is no longer registered in this app");

        var channelBindings = channel.WhatsApp?.ParameterBindings ?? new Dictionary<string, TelegramParameterBinding>();
        if (WhatsAppParameterBindings.TryBind(
                config, channelBindings, WhatsAppConversationId.SenderDigits(sender),
                out var parameters, out var bindError) == false)
        {
            await TrySendAsync(database, channelId, sender, ErrorReply, ct);
            throw new InvalidOperationException(bindError);
        }

        // WhatsApp has no streaming primitive: chunks only accumulate as a fallback
        // for agents whose output shape yields no final reply field.
        var accumulated = new StringBuilder();

        try
        {
            var result = await router.RunAsync(
                new AgentRequest(database, config.Identifier, conversationId, prompt, channel.Id!, parameters),
                chunk =>
                {
                    accumulated.Append(chunk);
                    return ValueTask.CompletedTask;
                },
                config, ct);

            var reply = string.IsNullOrWhiteSpace(result.Reply) ? accumulated.ToString() : result.Reply;
            if (string.IsNullOrWhiteSpace(reply))
            {
                logger.LogWarning("WhatsApp agent turn produced an empty reply for channel {ChannelId}", channel.Id);
                return;
            }

            await bridge.SendTextAsync(database, channelId, sender, reply, ct);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch
        {
            await TrySendAsync(database, channelId, sender, ErrorReply, ct);
            throw;
        }
    }

    // matches "/clear" and "/clear <anything>", mirroring the Telegram command shape
    private static bool IsClearCommand(string text)
    {
        var separator = text.IndexOfAny([' ', '\t', '\r', '\n']);
        var command = separator < 0 ? text : text[..separator];
        return command.Equals("/clear", StringComparison.OrdinalIgnoreCase);
    }

    private async Task ClearConversationAsync(string database, string conversationId, CancellationToken ct)
    {
        using var session = store.OpenAsyncSession(database);
        session.Delete(conversationId);
        session.Delete(ConversationPreview.IdFor(conversationId));
        await session.SaveChangesAsync(ct);
    }

    private async Task TrySendAsync(string database, string channelId, string sender, string text, CancellationToken ct)
    {
        try
        {
            await bridge.SendTextAsync(database, channelId, sender, text, ct);
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            logger.LogWarning(
                "WhatsApp send failed for channel {ChannelId} sender ...{SenderSuffix}: {Error}",
                channelId, SenderSuffix(sender), e.Message);
        }
    }

    // phone numbers are PII: log lines carry only the last four digits
    private static string SenderSuffix(string sender)
    {
        var digits = WhatsAppConversationId.SenderDigits(sender);
        return digits.Length <= 4 ? digits : digits[^4..];
    }
}
