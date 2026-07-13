using Raven.Client.Documents;
using Raven.Client.Documents.AI;
using Raven.Client.Documents.Operations.AI.Agents;

namespace Raven.Quill.Agents;

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
/// <paramref name="onChunk"/>. Callers that already resolved the agent (for a
/// pre-stream 4xx) pass it via <c>resolved</c> so the router skips a second lookup.
/// </summary>
public interface IAgentRouter
{
    Task<AgentRunResult> RunAsync(AgentRequest request, Func<string, ValueTask> onChunk, CancellationToken ct, AiAgentConfiguration? resolved = null);
}

/// <summary>Thrown when a request names an agent that doesn't exist in the
/// target per-app database. Endpoints map this to a 400 (or 404) instead of
/// leaking a 500.</summary>
public sealed class UnknownAgentException(string agentId)
    : Exception($"unknown agentId '{agentId}'")
{
    public string AgentId { get; } = agentId;
}

/// <summary>
/// Default <see cref="IAgentRouter"/>. Resolves the operator-provisioned agent from the
/// request's <em>per-app</em> database via <see cref="AgentLookup.FindAsync"/> — which lists
/// agents (<c>GetAgentsAsync</c>) and matches case-insensitively, deliberately avoiding the
/// single-id <c>GetAgentAsync</c> (it throws on a miss and is case-sensitive). Streams the reply
/// over a data-driven answer type, deriving the reply field at runtime from the persisted output
/// shape (<see cref="AgentOutputShape"/>). The embed chat and <c>/setup/try</c> feed through here;
/// the legacy <c>/api/chat/stream</c> resolves against the config database.
/// </summary>
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

        // Stream over a generic answer type so there is no compile-time output
        // shape. The done-frame answer is a controlled { reply } object the
        // appliance owns: the RavenDB client deserializes the model output via
        // Newtonsoft, so nested fields would arrive as JArray/JObject the wire
        // serializer (System.Text.Json) can't render. The streamed chunks remain
        // the primary reply path; this just gives clients a clean fallback.
        var result = await conversation.StreamAsync<Dictionary<string, object>>(
            replyField,
            async chunk => await onChunk(chunk),
            ct);

        var reply = AgentOutputShape.ExtractReplyText(result.Answer, replyField);
        return new AgentRunResult(new { reply }, conversation.Id);
    }

    /// <summary>
    /// Pin client-supplied conversation ids to the <c>chats/</c> prefix so a
    /// caller can't pass e.g. <c>users/admin</c> and overwrite an unrelated
    /// document. Empty/null lets RavenDB auto-allocate.
    /// </summary>
    internal static string NormalizeConversationId(string? conversationId)
    {
        if (TryNormalizeConversationId(conversationId, out var normalized, out var error) == false)
            throw new ArgumentException(error, nameof(conversationId));

        return normalized;
    }

    /// <summary>
    /// The single definition of the conversation-id rule: trim (so a
    /// stray-whitespace id like <c>"chats/1 "</c> isn't persisted verbatim);
    /// null/whitespace normalizes to <c>"chats/"</c> (RavenDB auto-allocates);
    /// anything else must start with <c>chats/</c>. Endpoints call this for a
    /// clean 400 before their NDJSON stream opens;
    /// <see cref="NormalizeConversationId"/> stays as the in-router safety net.
    /// </summary>
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
