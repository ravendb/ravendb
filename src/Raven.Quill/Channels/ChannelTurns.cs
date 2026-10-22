using System.Threading.Channels;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Agents;
using Raven.Quill.Hosting;
using Sparrow.Logging;

namespace Raven.Quill.Channels;

internal sealed record ChannelReplies(string Error, string Overloaded)
{
    internal const string UnsupportedKind = "I can only read text messages right now.";

    internal static readonly ChannelReplies Default = new(
        Error: "Sorry - something went wrong handling that message. Please try again.",
        Overloaded:
        "I'm still working through your earlier messages, so that one didn't make it. Please resend it once I've replied.");
}

internal interface IPlatformTurns<TMessage>
    where TMessage : IChannelMessage
{
    ChannelReplies Replies => ChannelReplies.Default;

    Task SendAsync(TMessage replyTo, string text, CancellationToken ct);

    ChannelStreamingReply CreateReply(TMessage replyTo);

    Task TypingAsync(TMessage replyTo, CancellationToken ct) => Task.CompletedTask;

    string? DescribeApiFailure(Exception e) => null;

    Task HandleCommandAsync(TMessage message, CancellationToken ct) =>
        throw new NotSupportedException($"{GetType().Name} has no commands");

    Task<Dictionary<string, string>?> BindAsync(AiAgentConfiguration config, TMessage message, CancellationToken ct);
}

internal sealed class ChannelTurns<TMessage>(
    IPlatformTurns<TMessage> platform,
    string database,
    Channel channel,
    ChannelConnectionHealth? health,
    IDocumentStore store,
    IAgentRouter router,
    ApplianceOptions options,
    IRavenLogger logger) : IChatTurns<TMessage>
    where TMessage : IChannelMessage
{
    // merge queued prompts, by design
    public async Task RunBatchAsync(ChannelReader<TMessage> queue, CancellationToken ct)
    {
        var prompts = new List<TMessage>();

        while (queue.TryRead(out var message))
        {
            if (message.Kind == ChannelMessageKind.Prompt)
            {
                if (string.IsNullOrWhiteSpace(message.Text) == false)
                    prompts.Add(message);
                continue;
            }

            await RunTurnAsync(prompts, ct);
            await RunTurnAsync([message], ct);
        }

        await RunTurnAsync(prompts, ct);
    }

    private async Task RunTurnAsync(List<TMessage> prompts, CancellationToken ct)
    {
        if (prompts.Count == 0)
            return;

        var last = prompts[^1];
        try
        {
            switch (last.Kind)
            {
                case ChannelMessageKind.Prompt:
                    await RunAgentAsync(prompts, ct);
                    break;

                case ChannelMessageKind.Command:
                    await platform.HandleCommandAsync(last, ct);
                    break;

                case ChannelMessageKind.Unsupported:
                    await TrySendAsync(last, ChannelReplies.UnsupportedKind, ct);
                    break;
            }
        }
        catch (Exception e) when (ct.IsCancellationRequested == false)
        {
            if (logger.IsWarnEnabled)
                logger.Warn($"{channel.Type} turn failed for channel {channel.ShortId} sender {last.SenderId}: {e.Message}");

            await TrySendAsync(last, platform.Replies.Error, ct);

            if (platform.DescribeApiFailure(e) is { } failure)
                health?.MarkSendFailed(failure);
        }
        finally
        {
            prompts.Clear();
        }
    }

    private async Task RunAgentAsync(IReadOnlyList<TMessage> messages, CancellationToken ct)
    {
        var last = messages[^1];
        var prompt = string.Join('\n', messages.Select(m => m.Text!.Trim()));

        var config = await AgentLookup.FindAsync(store, database, channel.AgentId, ct)
                     ?? throw new InvalidOperationException(
                         $"agent '{channel.AgentId}' is no longer registered in this app");

        var parameters = await platform.BindAsync(config, last, ct);
        if (parameters is null)
            return;

        var conversationId = ChannelConversationId.For(channel.Type, channel.ShortId, last.SenderId, parameters);

        await platform.TypingAsync(last, ct);
        var reply = platform.CreateReply(last);

        await router.RunAsync(
            new AgentRequest(database, config.Identifier, conversationId, prompt, channel.Id!,
                parameters.ToDictionary(
                    parameter => parameter.Key,
                    parameter => AgentParameterValue.FromString(parameter.Value)),
                options.ChannelConversationIdleWindow),
            chunk => reply.OnChunkAsync(chunk, ct), config, ct);

        await reply.FinalizeAsync(ct);
        health?.MarkSendSucceeded();

        if (reply.IsEmpty && logger.IsWarnEnabled)
            logger.Warn($"{channel.Type} agent turn produced an empty reply for channel {channel.Id}");
    }

    public Task NotifyBufferFullAsync(TMessage message, CancellationToken ct) =>
        TrySendAsync(message, platform.Replies.Overloaded, ct);

    private async Task TrySendAsync(TMessage replyTo, string text, CancellationToken ct)
    {
        try
        {
            await platform.SendAsync(replyTo, text, ct);
            health?.MarkSendSucceeded();
        }
        catch (Exception e) when (ct.IsCancellationRequested == false)
        {
            if (platform.DescribeApiFailure(e) is { } failure)
                health?.MarkSendFailed(failure);

            if (logger.IsWarnEnabled)
                logger.Warn($"{channel.Type} send failed for channel {channel.ShortId}: {e.Message}");
        }
    }
}
