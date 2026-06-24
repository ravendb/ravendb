namespace Raven.AiAppliance.Contracts;

/// <summary>
/// Agents-view statistics: the configured-agent count plus windowed usage
/// totals across all agents and a per-agent breakdown. Invocation counts and
/// token usage are aggregated from the <c>@conversations</c> collection;
/// per-turn execution times are added later via the metrics recorder.
/// </summary>
/// <param name="ConfiguredAgents">Number of agents provisioned in the app's RavenDB AI agent registry.</param>
/// <param name="Last24h">Usage totals across all agents for the last 24 hours.</param>
/// <param name="Last7d">Usage totals for the last 7 days.</param>
/// <param name="Last30d">Usage totals for the last 30 days.</param>
/// <param name="Agents">Per-agent usage over the last 30 days, ordered by agent id.</param>
public sealed record AgentStatsResponse(
    int ConfiguredAgents,
    ConversationWindow Last24h,
    ConversationWindow Last7d,
    ConversationWindow Last30d,
    AgentUsageSummary[] Agents);

/// <param name="AgentId">The agent identifier recorded on the conversations.</param>
/// <param name="Conversations">Conversations attributed to this agent in the window.</param>
/// <param name="Messages">Total messages across those conversations.</param>
/// <param name="Tokens">Total token usage across those conversations.</param>
public sealed record AgentUsageSummary(string AgentId, long Conversations, long Messages, long Tokens);
