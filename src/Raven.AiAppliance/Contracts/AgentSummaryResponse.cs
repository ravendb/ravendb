namespace Raven.AiAppliance.Contracts;

/// <summary>
/// Dashboard-facing agent summary for the Agents table (name, status, model) plus
/// usage from the conversation index: <paramref name="Invocations"/> and
/// <paramref name="LastInvokedAt"/>. <paramref name="SuccessRate"/> is 0 for now —
/// no per-turn outcome is captured yet (gap #1).
/// </summary>
/// <param name="AgentId">The agent's RavenDB identifier.</param>
/// <param name="Name">Operator-friendly name; falls back to the identifier when unset.</param>
/// <param name="Model">The chat model from the agent's connection string, or
/// null when the connection string is missing / carries no model.</param>
/// <param name="Disabled">Whether the agent is disabled (the dashboard renders
/// the inverse as an "Active" status).</param>
/// <param name="Parameters">Names of the agent's declared chat-scoped parameters.</param>
/// <param name="Invocations">Conversations attributed to this agent (all-time).</param>
/// <param name="SuccessRate">0..1; always 0 until per-turn outcomes are tracked.</param>
/// <param name="LastInvokedAt">Latest activity hour for this agent, or null.</param>
public sealed record AgentSummaryResponse(
    string AgentId,
    string Name,
    string? Model,
    bool Disabled,
    string[] Parameters,
    long Invocations,
    double SuccessRate,
    DateTime? LastInvokedAt);
