namespace Raven.Quill.Contracts;

/// <summary>
/// Dashboard-facing agent summary for the merged Agents table: identity (name, status,
/// model), last activity, and per-agent usage from the conversation index
/// (<paramref name="Conversations"/> / <paramref name="Messages"/> / <paramref name="Tokens"/>).
/// </summary>
/// <param name="AgentId">The agent's RavenDB identifier.</param>
/// <param name="Name">Operator-friendly name; falls back to the identifier when unset.</param>
/// <param name="Model">The chat model from the agent's connection string, or
/// null when the connection string is missing / carries no model.</param>
/// <param name="Disabled">Whether the agent is disabled (the dashboard renders
/// the inverse as an "Active" status).</param>
/// <param name="Parameters">Names of the agent's declared chat-scoped parameters.</param>
/// <param name="LastInvokedAt">Latest activity hour for this agent, or null.</param>
/// <param name="Conversations">Conversations attributed to this agent (all-time).</param>
/// <param name="Messages">User messages across this agent's conversations (all-time).</param>
/// <param name="Tokens">Total tokens across this agent's conversations (all-time).</param>
public sealed record AgentSummaryResponse(
    string AgentId,
    string Name,
    string? Model,
    bool Disabled,
    string[] Parameters,
    DateTime? LastInvokedAt,
    long Conversations,
    long Messages,
    long Tokens);
