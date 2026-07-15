using System;

namespace Raven.Quill.Contracts;

public sealed record CdcPerformanceResponse(
    bool Enabled,
    string Status,
    DateTime? LastSyncAt,
    int? LagSeconds,
    long RecentReads,
    long RecentWrites,
    int ErrorCount,
    CdcBatchPoint[] RecentBatches);

public sealed record CdcBatchPoint(
    DateTime Started,
    DateTime? Completed,
    double DurationInMs,
    int Read,
    int Processed,
    int Errors,
    string? StopReason);

/// <summary>One error from the sink's persistent error store (RavenDB's
/// <c>cdc-sink/errors</c>), served on demand by <c>GET /api/apps/{slug}/cdc/errors</c>.
/// <c>DocumentId</c> is set for item-level errors, <c>AffectedDocumentsCount</c> for
/// process-level ones; the other is null. <c>Step</c> is the pipeline stage that failed.</summary>
public sealed record CdcError(
    string TaskName,
    DateTime CreatedAt,
    string Step,
    string Error,
    string? DocumentId,
    long? AffectedDocumentsCount);
