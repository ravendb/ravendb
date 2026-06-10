namespace Raven.AiAppliance.Contracts;

/// <summary>
/// Dashboard-facing agent summary for the app overview. Curated like
/// <see cref="ChannelSummaryResponse"/>: only the fields the Agents table needs
/// (name, status, model). Run counts / last-run are intentionally absent — the
/// platform tracks no per-agent usage stats yet.
/// </summary>
/// <param name="AgentId">The agent's RavenDB identifier.</param>
/// <param name="Name">Operator-friendly name; falls back to the identifier when unset.</param>
/// <param name="Model">The chat model from the agent's connection string, or
/// null when the connection string is missing / carries no model.</param>
/// <param name="Disabled">Whether the agent is disabled (the dashboard renders
/// the inverse as an "Active" status).</param>
/// <param name="Parameters">Names of the agent's declared chat-scoped
/// parameters — the values a caller must supply to open a conversation. The
/// channel preview uses this to collect values before loading the widget.</param>
public sealed record AgentSummaryResponse(
    string AgentId,
    string Name,
    string? Model,
    bool Disabled,
    string[] Parameters);
