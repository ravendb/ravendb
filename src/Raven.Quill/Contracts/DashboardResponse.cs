namespace Raven.Quill.Contracts;

/// <summary>
/// Global dashboard roll-up: app count plus conversation aggregates for the calendar period
/// selected by year / year+month / year+month+day, summed across every app database (read-time
/// fan-out). Mirrors the usage endpoints' period model. Ingestion/write totals are added later
/// (they need the metrics recorder / CDC perf stats).
/// </summary>
/// <param name="Apps">Number of provisioned apps.</param>
/// <param name="Conversations">Conversations across all apps in the period.</param>
/// <param name="Messages">Total messages across all apps in the period.</param>
/// <param name="Tokens">Total token usage across all apps in the period.</param>
public sealed record DashboardResponse(int Apps, long Conversations, long Messages, long Tokens);
