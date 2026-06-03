using Raven.AiAppliance.Schema;
using Raven.Client.Documents;
using Raven.Client.Documents.AI;

namespace Raven.AiAppliance.Agents;

/// <summary>
/// Normalized request every channel funnels into the agent. Channel adapters
/// (iFrame HTTP now; Telegram polling later — design §4) handle only transport,
/// auth, and channel-instance lookup, then converge here.
/// </summary>
/// <param name="Database">The per-app RavenDB database to route against.</param>
/// <param name="AgentId">The agent identifier inside that database.</param>
/// <param name="ConversationId">Stable per (channel, end-user, epoch window);
/// <c>null</c> lets RavenDB allocate a fresh <c>chats/</c> id.</param>
/// <param name="Prompt">The end-user's text.</param>
/// <param name="Parameters">Agent parameters (e.g. <c>Customer</c>,
/// <c>UserIdentifier</c>) resolved from the channel's identifier source.</param>
public sealed record AgentRequest(
    string Database,
    string AgentId,
    string? ConversationId,
    string Prompt,
    IReadOnlyDictionary<string, string>? Parameters = null);

/// <summary>The agent's final structured answer plus the conversation id it
/// ran under (so the caller can echo it in the stream's <c>done</c> frame and
/// the client can continue the same thread on the next turn).</summary>
public sealed record AgentRunResult(object Answer, string ConversationId);

/// <summary>
/// The single funnel every channel feeds into (design §4). Translates an
/// <see cref="AgentRequest"/> into a RavenDB AI conversation against the
/// <em>per-app</em> database and streams reply chunks back through
/// <paramref name="onChunk"/>.
/// </summary>
public interface IAgentRouter
{
    Task<AgentRunResult> RunAsync(AgentRequest request, Func<string, ValueTask> onChunk, CancellationToken ct);
}

/// <summary>Thrown when a request names an agent the registry doesn't know.
/// Endpoints map this to a 400 (or 404) instead of leaking a 500.</summary>
public sealed class UnknownAgentException(string agentId)
    : Exception($"unknown agentId '{agentId}'")
{
    public string AgentId { get; } = agentId;
}

/// <summary>
/// Default <see cref="IAgentRouter"/>. Resolves the agent's
/// <see cref="IAgentSchema"/> from the in-process registry (for the stream
/// property path + answer type) but opens the conversation against the
/// request's per-app database via <c>store.AI.ForDatabase(...)</c> — the demo
/// pins each per-app agent's identifier to a registered schema identifier
/// (e.g. <c>demo-agent</c>), so the registry schema and the per-app agent line
/// up. The legacy <c>/api/chat/stream</c> ran against the config DB; this
/// router is what the embed chat and <c>/setup/try</c> use to reach the agent
/// the operator actually provisioned.
/// </summary>
internal sealed class AgentRouter(IDocumentStore store, IAgentSchemaRegistry schemas) : IAgentRouter
{
    public async Task<AgentRunResult> RunAsync(AgentRequest request, Func<string, ValueTask> onChunk, CancellationToken ct)
    {
        if (schemas.TryGet(request.AgentId, out var schema) == false)
            throw new UnknownAgentException(request.AgentId);

        var conversationId = NormalizeConversationId(request.ConversationId);

        var creationOptions = new AiConversationCreationOptions();
        if (request.Parameters is not null)
        {
            foreach (var (key, value) in request.Parameters)
                creationOptions.AddParameter(key, value);
        }

        var conversation = store.AI.ForDatabase(request.Database).Conversation(
            agentId: schema.Identifier,
            conversationId: conversationId,
            creationOptions: creationOptions);

        conversation.AddUserPrompt(request.Prompt);

        var answer = await schema.RunConversationAsync(conversation, onChunk, ct);
        return new AgentRunResult(answer, conversation.Id);
    }

    /// <summary>
    /// Pin client-supplied conversation ids to the <c>chats/</c> prefix so a
    /// caller can't pass e.g. <c>users/admin</c> and overwrite an unrelated
    /// document. Empty/null lets RavenDB auto-allocate.
    /// </summary>
    internal static string NormalizeConversationId(string? conversationId)
    {
        // Trim first so a stray-whitespace id (e.g. "chats/1 ") isn't persisted
        // verbatim as a distinct RavenDB conversation document.
        conversationId = conversationId?.Trim();

        if (string.IsNullOrWhiteSpace(conversationId))
            return "chats/";

        if (conversationId.StartsWith("chats/", StringComparison.Ordinal) == false)
            throw new ArgumentException("conversationId must start with 'chats/'", nameof(conversationId));

        return conversationId;
    }
}
