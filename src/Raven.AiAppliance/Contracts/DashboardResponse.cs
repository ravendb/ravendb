namespace Raven.AiAppliance.Contracts;

/// <summary>
/// Global dashboard roll-up: app count plus windowed conversation aggregates
/// summed across every app database (read-time fan-out). Ingestion/write totals
/// are added later (they need the metrics recorder / CDC perf stats).
/// </summary>
/// <param name="Apps">Number of provisioned apps.</param>
/// <param name="Last24h">Conversation aggregates across all apps for the last 24 hours.</param>
/// <param name="Last7d">Across all apps for the last 7 days.</param>
/// <param name="Last30d">Across all apps for the last 30 days.</param>
public sealed record DashboardResponse(
    int Apps,
    ConversationWindow Last24h,
    ConversationWindow Last7d,
    ConversationWindow Last30d);
