using Microsoft.Extensions.Options;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Agents;
using Raven.Quill.Hosting;
using Sparrow.Logging;

namespace Raven.Quill.Channels;

internal sealed record ChannelReplies(string Error, string ConversationExpired, string Overloaded)
{
    internal const string UnsupportedKind = "I can only read text messages right now.";

    internal static readonly ChannelReplies Default = new(
        Error: "Sorry - something went wrong handling that message. Please try again.",
        ConversationExpired:
        "By the way, this is a fresh conversation - our previous one ended after a period of inactivity, so I no longer have its context.",
        Overloaded:
        "I'm still working through your earlier messages, so that one didn't make it. Please resend it once I've replied.");
}

internal interface IChannelBot : IAsyncDisposable
{
    ChannelReplies Replies => ChannelReplies.Default;

    ChannelConnectionHealth? Health => null;

    Task SendAsync(string text, CancellationToken ct);

    ChannelStreamingReply CreateReply();

    string? DescribeApiFailure(Exception e) => null;

    Task TypingAsync(CancellationToken ct) => Task.CompletedTask;
}

internal interface IPlatformTurns<TMessage, TBot>
    where TMessage : IChannelMessage
    where TBot : class, IChannelBot
{
    ChannelType Type { get; }

    TBot OpenBot(Channel channel, TMessage message);

    Task<bool> TryHandleAsync(TBot bot, Channel channel, TMessage message, CancellationToken ct) =>
        Task.FromResult(false);

    Task<Dictionary<string, string>?> BindAsync(
        TBot bot, Channel channel, AiAgentConfiguration config, TMessage message, CancellationToken ct);
}

internal sealed class ChannelTurns<TMessage, TBot>(
    IPlatformTurns<TMessage, TBot> platform,
    IDocumentStore store,
    IAgentRouter router,
    IOptions<ApplianceOptions> options,
    IRavenLogger logger) : IChatTurns<TMessage>
    where TMessage : IChannelMessage
    where TBot : class, IChannelBot
{
    public async Task RunTurnAsync(IReadOnlyList<TMessage> messages, CancellationToken ct)
    {
        var last = messages[^1];
        var channel = last.Channel;

        await using var bot = platform.OpenBot(channel, last);

        if (messages is [{ RunsAlone: true } only])
        {
            if (only.IsUnsupported)
            {
                await TrySendAsync(bot, channel, ChannelReplies.UnsupportedKind, ct);
                return;
            }

            if (await platform.TryHandleAsync(bot, channel, only, ct))
                return;
        }

        var prompt = string.Join('\n', messages.Select(m => (m.Text ?? "").Trim()).Where(t => t.Length > 0));
        if (prompt.Length == 0)
            return;

        var config = await AgentLookup.FindAsync(store, last.Database, channel.AgentId, ct)
                     ?? throw new InvalidOperationException(
                         $"agent '{channel.AgentId}' is no longer registered in this app");

        try
        {
            var parameters = await platform.BindAsync(bot, channel, config, last, ct);
            if (parameters is null)
                return;

            var conversationId = ChannelConversationId.For(platform.Type, channel.ShortId, last.SenderId, parameters);

            await bot.TypingAsync(ct);
            var reply = bot.CreateReply();

            var result = await router.RunAsync(
                new AgentRequest(last.Database, config.Identifier, conversationId, prompt, channel.Id!,
                    parameters.ToDictionary(
                        parameter => parameter.Key,
                        parameter => AgentParameterValue.FromString(parameter.Value)),
                    options.Value.ChannelConversationIdleWindow),
                chunk => reply.OnChunkAsync(chunk, ct), config, ct);

            await reply.FinalizeAsync(ct);
            bot.Health?.MarkSendSucceeded();

            if (result.StartedFresh)
                await TrySendAsync(bot, channel, bot.Replies.ConversationExpired, ct);

            if (reply.IsEmpty && logger.IsWarnEnabled)
                logger.Warn($"{platform.Type} agent turn produced an empty reply for channel {channel.Id}");
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception e)
        {
            if (bot.DescribeApiFailure(e) is { } failure)
                bot.Health?.MarkSendFailed(failure);

            await TrySendAsync(bot, channel, bot.Replies.Error, ct);
            throw;
        }
    }

    public async Task NotifyBufferFullAsync(TMessage message, CancellationToken ct)
    {
        try
        {
            await using var bot = platform.OpenBot(message.Channel, message);
            await TrySendAsync(bot, message.Channel, bot.Replies.Overloaded, ct);
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            if (logger.IsDebugEnabled)
                logger.Debug($"{platform.Type} overload notice failed for channel {message.Channel.Id}: {e.Message}");
        }
    }

    private async Task TrySendAsync(TBot bot, Channel channel, string text, CancellationToken ct)
    {
        try
        {
            await bot.SendAsync(text, ct);
            bot.Health?.MarkSendSucceeded();
        }
        catch (Exception e) when (ct.IsCancellationRequested == false)
        {
            if (bot.DescribeApiFailure(e) is { } failure)
                bot.Health?.MarkSendFailed(failure);

            if (logger.IsWarnEnabled)
                logger.Warn($"{platform.Type} send failed for channel {channel.ShortId}: {e.Message}");
        }
    }
}
