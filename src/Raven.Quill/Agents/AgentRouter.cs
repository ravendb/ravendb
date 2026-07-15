using Raven.Client.Documents;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI.Agents;

namespace Raven.Quill.Agents;

public sealed record AgentRequest(
    string Database,
    string AgentId,
    string? ConversationId,
    string Prompt,
    IReadOnlyDictionary<string, string>? Parameters = null);

public sealed record AgentRunResult(object Answer, string ConversationId);

public interface IAgentRouter
{
    Task<AgentRunResult> RunAsync(AgentRequest request, Func<string, ValueTask> onChunk, CancellationToken ct, AiAgentConfiguration? resolved = null);
}

public sealed class UnknownAgentException(string agentId)
    : Exception($"unknown agentId '{agentId}'")
{
    public string AgentId { get; } = agentId;
}

internal sealed class AgentRouter(IDocumentStore store) : IAgentRouter
{
    public async Task<AgentRunResult> RunAsync(AgentRequest request, Func<string, ValueTask> onChunk, CancellationToken ct, AiAgentConfiguration? resolved = null)
    {
        var config = resolved ?? await AgentLookup.FindAsync(store, request.Database, request.AgentId, ct);
        if (config is null)
            throw new UnknownAgentException(request.AgentId);

        var conversationId = NormalizeConversationId(request.ConversationId);

        var creationOptions = new AiConversationCreationOptions();
        if (request.Parameters is not null)
        {
            foreach (var (key, value) in request.Parameters)
                creationOptions.AddParameter(key, value);
        }

        var conversation = store.AI.ForDatabase(request.Database).Conversation(
            agentId: config.Identifier,
            conversationId: conversationId,
            creationOptions: creationOptions);

        conversation.AddUserPrompt(request.Prompt);

        var replyField = AgentOutputShape.ResolveReplyField(config);

        var result = await conversation.StreamAsync<Dictionary<string, object>>(
            replyField,
            async chunk => await onChunk(chunk),
            ct);

        var reply = AgentOutputShape.ExtractReplyText(result.Answer, replyField);
        return new AgentRunResult(new { reply }, conversation.Id);
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
