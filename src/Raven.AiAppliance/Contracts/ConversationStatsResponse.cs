namespace Raven.AiAppliance.Contracts;

/// <summary>
/// Conversations-view statistics: rolling-window aggregates over the per-app
/// <c>@conversations</c> collection. Windows are computed at request time from
/// the hour-bucketed <c>ConversationMetricsIndex</c>.
/// </summary>
/// <param name="Last24h">Aggregate over conversations created in the last 24 hours.</param>
/// <param name="Last7d">Aggregate over the last 7 days.</param>
/// <param name="Last30d">Aggregate over the last 30 days.</param>
public sealed record ConversationStatsResponse(
    ConversationWindow Last24h,
    ConversationWindow Last7d,
    ConversationWindow Last30d);

/// <param name="Conversations">Number of conversations created in the window.</param>
/// <param name="Messages">Total messages across those conversations.</param>
/// <param name="Tokens">Total token usage across those conversations.</param>
public sealed record ConversationWindow(long Conversations, long Messages, long Tokens);
