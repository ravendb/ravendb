namespace Raven.AiAppliance.Contracts;

/// <summary>
/// Channels-view counts. Conversation-volume-over-time and the
/// unrouted/dead-letter count are added later (the latter needs the metrics
/// recorder, since nothing is persisted for it today).
/// </summary>
/// <param name="Total">Total channels provisioned in the app.</param>
/// <param name="Active">Channels that are enabled (not paused).</param>
public sealed record ChannelStatsResponse(int Total, int Active);
