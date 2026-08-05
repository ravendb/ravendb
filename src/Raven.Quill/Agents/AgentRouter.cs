using Raven.Client.Documents;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI.Agents;
using Raven.Quill.Metrics;

namespace Raven.Quill.Agents;

public sealed record AgentRequest(
    string Database,
    string AgentId,
    string ConversationId,
    string Prompt,
    string ChannelId,
    IReadOnlyDictionary<string, string> Parameters);

public sealed record AgentRunResult(object Answer, string ConversationId);

public interface IAgentRouter
{
    Task<AgentRunResult> RunAsync(AgentRequest request, Func<string, ValueTask> onChunk, AiAgentConfiguration config, CancellationToken ct);
}

public sealed class UnknownAgentException(string agentId)
    : Exception($"unknown agentId '{agentId}'")
{
    public string AgentId { get; } = agentId;
}

internal sealed class AgentRouter(
    IDocumentStore store, WebhookActionExecutor actionExecutor, ILogger<AgentRouter> logger) : IAgentRouter
{
    public async Task<AgentRunResult> RunAsync(AgentRequest request, Func<string, ValueTask> onChunk, AiAgentConfiguration config, CancellationToken ct)
    {
        if (config is null)
            throw new UnknownAgentException(request.AgentId);

        var conversationId = NormalizeConversationId(request.ConversationId);

        var creationOptions = new AiConversationCreationOptions();
        foreach (var (key, value) in request.Parameters)
            creationOptions.AddParameter(key, value);

        var conversation = store.AI.ForDatabase(request.Database).Conversation(
            agentId: config.Identifier,
            conversationId: conversationId,
            creationOptions: creationOptions);

        conversation.AddUserPrompt(request.Prompt);

        await RegisterActionsAsync(conversation, request, config, ct);

        var replyField = AgentOutputShape.ResolveReplyField(config);

        var result = await conversation.StreamAsync<Dictionary<string, object>>(
            replyField,
            async chunk => await onChunk(chunk),
            ct);

        // every action gets a response above, so the client's turn loop only returns once done
        if (result.Status == AiConversationResult.ActionRequired)
            throw new InvalidOperationException(
                $"conversation '{conversation.Id}' still requires actions after the turn completed");

        var reply = AgentOutputShape.ExtractReplyText(result.Answer, replyField);

        await UpsertPreviewAsync(store, request, config.Identifier, conversation.Id, reply, DateTime.UtcNow, ct);

        return new AgentRunResult(new { reply }, conversation.Id);
    }

    private async Task RegisterActionsAsync(
        IAiConversationOperations conversation, AgentRequest request, AiAgentConfiguration config,
        CancellationToken ct)
    {
        conversation.OnUnhandledAction += args =>
        {
            logger.LogWarning(
                "Agent '{AgentId}' invoked action '{Action}' (toolId {ToolId}) with no binding configured",
                config.Identifier, args.Action.Name, args.Action.ToolId);

            conversation.AddActionResponse(args.Action.ToolId,
                $"action failed: no binding configured for '{args.Action.Name}'");
            return Task.CompletedTask;
        };

        if (config.Actions is not { Count: > 0 })
            return;

        AgentActionBindings? bindings;
        using (var session = store.OpenAsyncSession(request.Database))
            bindings = await session.LoadAsync<AgentActionBindings>(AgentActionBindings.IdFor(config.Identifier), ct);

        if (bindings is null)
            return;

        foreach (var action in config.Actions)
        {
            if (bindings.Bindings.TryGetValue(action.Name, out var binding) == false)
                continue;

            conversation.Receive(action.Name, async actionRequest =>
            {
                var response = await actionExecutor.ExecuteAsync(actionRequest, binding, ct);

                conversation.AddActionResponse(actionRequest.ToolId, response);
            });
        }
    }

    internal static async Task UpsertPreviewAsync(
        IDocumentStore store, AgentRequest request, string agent, string conversationId, string reply,
        DateTime nowUtc, CancellationToken ct)
    {
        using var session = store.OpenAsyncSession(request.Database);
        var id = ConversationPreview.IdFor(conversationId);
        var preview = await session.LoadAsync<ConversationPreview>(id, ct) ?? new ConversationPreview
        {
            ConversationId = conversationId,
            Agent = agent,
            ChannelId = request.ChannelId,
            Parameters = new Dictionary<string, string>(request.Parameters),
            CreatedAt = nowUtc
        };

        preview.LastMessageAt = nowUtc;
        preview.LastUserPrompt = request.Prompt;
        preview.LastAgentReply = reply;

        await session.StoreAsync(preview, id, ct);
        await session.SaveChangesAsync(ct);
        return;
    }

    internal static string NormalizeConversationId(string? conversationId)
    {
        if (TryNormalizeConversationId(conversationId, out var normalized, out var error) == false)
            throw new ArgumentException(error, nameof(conversationId));

        return normalized;
    }

    internal static bool TryNormalizeConversationId(string? raw, out string normalized, out string? error)
    {
        var trimmed = raw?.Trim();

        if (string.IsNullOrWhiteSpace(trimmed))
        {
            normalized = "chats/";
            error = null;
            return true;
        }

        // pin to the chats/ prefix so a caller can't overwrite another document
        if (trimmed.StartsWith("chats/", StringComparison.Ordinal) == false)
        {
            normalized = "";
            error = "conversationId must start with 'chats/'";
            return false;
        }

        normalized = trimmed;
        error = null;
        return true;
    }
}
