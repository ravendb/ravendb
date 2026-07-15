namespace Raven.Quill.Contracts;

/// <summary>
/// Conversations-view statistics: totals over the per-app <c>@conversations</c> collection
/// for the calendar period selected by year / year+month / year+month+day, aggregated from
/// the hour-bucketed <c>ConversationMetricsIndex</c>. Mirrors the usage endpoints' period model.
/// </summary>
/// <param name="Conversations">Number of conversations created in the period.</param>
/// <param name="Messages">Total messages across those conversations.</param>
/// <param name="Tokens">Total token usage across those conversations.</param>
public sealed record ConversationStatsResponse(long Conversations, long Messages, long Tokens);
