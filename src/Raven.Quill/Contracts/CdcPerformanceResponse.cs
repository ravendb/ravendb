using System;

namespace Raven.Quill.Contracts;

/// <summary>
/// CDC sink performance snapshot for an app's CDC page — derived from the server's rolling
/// per-batch perf stats (last ~25 batches). <c>enabled</c>/<c>status</c> reflect the CDC
/// config; the batch-derived fields stay empty/zero until the server collects stats
/// (RavenDB-26780 / ravendb#23046) and a batch has run. For live updates the page also reads
/// the <c>/cdc/progress</c> WebSocket feed. This is a recent-activity snapshot, not a
/// historical time series — <c>cdcWrites</c>/per-table lag/monthly totals have no source yet.
/// </summary>
/// <param name="Status">"not-configured" | "disabled" | "idle" | "active" | "error".</param>
/// <param name="LastSyncAt">Newest batch completion (UTC), or null if none completed.</param>
/// <param name="LagSeconds">Seconds since the last completed batch, or null.</param>
public sealed record CdcPerformanceResponse(
    bool Enabled,
    string Status,
    DateTime? LastSyncAt,
    int? LagSeconds,
    long RecentReads,
    long RecentWrites,
    int ErrorCount,
    CdcBatchPoint[] RecentBatches);

/// <summary>One CDC batch from the server's rolling window.</summary>
public sealed record CdcBatchPoint(
    DateTime Started,
    DateTime? Completed,
    double DurationInMs,
    int Read,
    int Processed,
    int Errors,
    string? StopReason);
